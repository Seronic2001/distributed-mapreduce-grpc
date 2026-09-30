// Q8 weather analytics — Hadoop Streaming MAP phase (Section 2 Q1).
//
// Input (stdin): this mapper's split of the weather dataset. The first line
// of the file ("N K S") is recognized by the 3-token shape (records always
// have 7 fields) and is skipped by whichever mapper receives it; that mapper
// announces K to the reducer with a single "meta" pair.
//
// Computation: the mapper aggregates its records locally into a Partial
// (sums, minima, maxima, extremes, hottest/coldest candidate, interval
// histogram, per-station accumulators) and flushes one wire-format block
// every Q8MR_FLUSH records (default 200000). Flushing bounds memory and
// gives the combiner several blocks to merge per mapper.
//
// Output: "tag\tfields" pairs consumed by the combiner/reducer (see
// common.h for the wire format). No inter-worker communication happens
// here: each mapper works only on its own split, like the MPI ranks do.

#include "common.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

using namespace q8mr;

namespace {

// Split one line into whitespace-separated tokens (pointers into the line).
int tokenize(char* line, char** tokens, int maxTokens) {
    int count = 0;
    char* cursor = line;
    while (*cursor != '\0' && count < maxTokens) {
        while (*cursor == ' ' || *cursor == '\t' || *cursor == '\r') ++cursor;
        if (*cursor == '\0') break;
        tokens[count++] = cursor;
        while (*cursor != '\0' && *cursor != ' ' && *cursor != '\t' &&
               *cursor != '\r')
            ++cursor;
        if (*cursor != '\0') *cursor++ = '\0';
    }
    return count;
}

}  // namespace

int main() {
    const long long flushEvery = std::getenv("Q8MR_FLUSH")
                                     ? std::atoll(std::getenv("Q8MR_FLUSH"))
                                     : 200000;

    Partial local;
    long long topK = 10;
    bool sawHeader = false;
    bool emittedMeta = false;
    std::string line;
    char* tokens[16];

    while (std::getline(std::cin, line)) {
        if (line.empty()) continue;
        std::vector<char> buffer(line.begin(), line.end());
        buffer.push_back('\0');
        const int tokenCount = tokenize(buffer.data(), tokens, 16);
        if (tokenCount == 3) {
            // File header "N K S": only the first mapper's split contains it.
            if (!sawHeader) {
                topK = std::atoll(tokens[1]);
                sawHeader = true;
                if (!emittedMeta) {
                    std::cout << "meta\tK=" << topK << "\n";
                    emittedMeta = true;
                }
            }
            continue;
        }
        if (tokenCount != 7) continue;  // ignore malformed lines
        const long long ts = std::strtoll(tokens[0], nullptr, 10);
        const long long station = std::strtoll(tokens[1], nullptr, 10);
        const double t = std::strtod(tokens[2], nullptr);
        const double h = std::strtod(tokens[3], nullptr);
        const double p = std::strtod(tokens[4], nullptr);
        const double r = std::strtod(tokens[5], nullptr);
        const double w = std::strtod(tokens[6], nullptr);
        local.addRecord(t, h, p, r, w, ts, station);
        if (local.count >= flushEvery) {
            local.emit(std::cout, false, topK);
            local = Partial();
        }
    }
    if (sawHeader && !emittedMeta) {
        std::cout << "meta\tK=" << topK << "\n";
        emittedMeta = true;
    }
    local.emit(std::cout, false, topK);
    return 0;
}
