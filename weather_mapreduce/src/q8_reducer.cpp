// Q8 weather analytics — Hadoop Streaming REDUCE phase (Section 2 Q1).
//
// Hadoop shuffles all mapper/combiner output to the reducer and groups it
// by key. All keys here are global tags ("count", "sums", ...) that merge
// associatively, so the whole stream must arrive at ONE reducer; the job
// launchers (run_local.sh, run_hadoop.sh) therefore fix
// mapreduce.job.reduces=1. (Multi-reducer would print one partial report
// per reducer and is not meaningful for a single global report.)
//
// The reducer merges every partial into one global Partial and prints the
// final 19-line report — byte-for-byte the same output as the baseline
// sequential and MPI programs.

#include "common.h"

using namespace q8mr;

int main() {
    Partial global;
    long long topK = 10;
    bool sawMeta = false;

    std::string line;
    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;
        const std::string::size_type tab = line.find('\t');
        if (tab == std::string::npos) continue;
        const std::string key = line.substr(0, tab);
        const std::string value = line.substr(tab + 1);
        if (key.rfind("meta", 0) == 0) {
            const std::string::size_type eq = value.find('=');
            if (eq != std::string::npos) {
                topK = std::atoll(value.c_str() + eq + 1);
                sawMeta = true;
            }
            continue;
        }
        if (!global.absorb(key, value)) {
            std::cerr << "reducer: unknown pair tag '" << key << "'\n";
            return 2;
        }
    }
    if (!sawMeta) topK = 10;
    printReport(std::cout, global, topK);
    return 0;
}
