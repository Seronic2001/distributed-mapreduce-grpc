// Q8 weather analytics — Hadoop Streaming COMBINE phase (Section 2 Q1).
//
// Hadoop runs the combiner on each mapper node over the mapper's spilled
// pairs, merging several Partial blocks into one before the shuffle. The
// combiner's output is again wire-format lines, so the reducer cannot tell
// whether it is merging raw mapper blocks or pre-merged combiner output —
// the same merge code path serves both.
//
// Memory is bounded by the number of distinct tags/stations/intervals per
// mapper, not by the number of records.

#include "common.h"

using namespace q8mr;

int main() {
    Partial merged;
    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;
        const std::string::size_type tab = line.find('\t');
        if (tab == std::string::npos) continue;
        const std::string key = line.substr(0, tab);
        if (key.rfind("meta", 0) == 0) {
            // Control pair: pass it through untouched so the reducer
            // still learns K (only one mapper emits it).
            std::cout << line << "\n";
            continue;
        }
        merged.absorb(key, line.substr(tab + 1));
    }
    merged.emit(std::cout, false, 0);
    return 0;
}
