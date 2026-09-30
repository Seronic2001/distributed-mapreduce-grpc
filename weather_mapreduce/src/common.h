// Shared aggregation logic for the Q8 weather MapReduce job (Section 2 Q1).
//
// All three programs (mapper, combiner, reducer) speak one wire format, so
// the combiner is literally the same merge code as the reducer's final
// aggregation. TAB separates key and value; single spaces separate fields.
//
//   meta\tK=<K>                  emitted once by the mapper that reads the header
//   count\t<n>                   measurement count
//   sums\t<T> <H> <P> <R> <W>    per-metric sums (temperature, humidity,
//                                pressure, rainfall, wind speed)
//   mins\t<T> <H> <P>            temperature/humidity/pressure minima
//   maxs\t<T> <H> <P> <R> <W>    per-metric maxima
//   extreme\t<n>                 extreme-temperature event count
//   hot\t<temp> <station> <ts>   local hottest candidate (tie rules applied)
//   cold\t<temp> <station> <ts>  local coldest candidate
//   iv\t<interval> <n>           busiest-interval histogram entry (ts/60)
//   st\t<id> <n> <tsum> <rsum>   per-station count / temp sum / rain sum
//
// Doubles are serialized with %.17g and parsed with strtod, which round-trips
// binary64 exactly: partial sums can then cross combiner/reducer boundaries
// without precision loss.
//
// Tie-breaking (same as the MPI implementation from baseline):
//   hottest/coldest : temperature first, then smaller timestamp, then
//                     smaller station id
//   busiest interval: larger count, then smaller interval id
//   top-K stations  : larger count, then smaller station id

#pragma once

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace q8mr {

const double POS_INF = std::numeric_limits<double>::infinity();
const double NEG_INF = -std::numeric_limits<double>::infinity();

// Exact binary64 round-trip: %.17g on output, strtod on input.
inline void appendDouble(std::string& out, double value) {
    char buffer[40];
    std::snprintf(buffer, sizeof buffer, "%.17g", value);
    out += buffer;
}

struct StationAgg {
    long long count = 0;
    double tempSum = 0.0;
    double rainSum = 0.0;
};

// One mapper's (or one reducer's) partial aggregate.
struct Partial {
    long long count = 0;
    double sums[5] = {0, 0, 0, 0, 0};   // T H P R W
    double mins[3] = {POS_INF, POS_INF, POS_INF};                   // T H P
    double maxs[5] = {NEG_INF, NEG_INF, NEG_INF, NEG_INF, NEG_INF}; // T H P R W
    long long extreme = 0;
    double hotTemp = NEG_INF; long long hotTs = 0; int hotStation = 0;
    double coldTemp = POS_INF; long long coldTs = 0; int coldStation = 0;
    bool hasHot = false;
    bool hasCold = false;
    std::map<long long, long long> intervals;   // interval -> count
    std::map<long long, StationAgg> stations;   // station -> aggregate

    static bool hotter(double t, long long ts, long long st,
                       double t2, long long ts2, long long st2) {
        if (t != t2) return t > t2;
        if (ts != ts2) return ts < ts2;
        return st < st2;
    }
    static bool colder(double t, long long ts, long long st,
                       double t2, long long ts2, long long st2) {
        if (t != t2) return t < t2;
        if (ts != ts2) return ts < ts2;
        return st < st2;
    }

    void addRecord(double t, double h, double p, double r, double w,
                   long long ts, long long station) {
        ++count;
        sums[0] += t; sums[1] += h; sums[2] += p; sums[3] += r; sums[4] += w;
        mins[0] = std::min(mins[0], t);
        mins[1] = std::min(mins[1], h);
        mins[2] = std::min(mins[2], p);
        maxs[0] = std::max(maxs[0], t);
        maxs[1] = std::max(maxs[1], h);
        maxs[2] = std::max(maxs[2], p);
        maxs[3] = std::max(maxs[3], r);
        maxs[4] = std::max(maxs[4], w);
        if (t >= 40.0 || t <= 0.0) ++extreme;
        if (!hasHot || hotter(t, ts, station, hotTemp, hotTs, hotStation)) {
            hotTemp = t; hotTs = ts; hotStation = (int)station; hasHot = true;
        }
        if (!hasCold || colder(t, ts, station, coldTemp, coldTs, coldStation)) {
            coldTemp = t; coldTs = ts; coldStation = (int)station; hasCold = true;
        }
        ++intervals[ts / 60];
        StationAgg& s = stations[station];
        ++s.count; s.tempSum += t; s.rainSum += r;
    }

    // Merge another partial into this one (combiner and reducer share it).
    void merge(const Partial& other) {
        count += other.count;
        for (int i = 0; i < 5; ++i) sums[i] += other.sums[i];
        for (int i = 0; i < 3; ++i) mins[i] = std::min(mins[i], other.mins[i]);
        for (int i = 0; i < 5; ++i) maxs[i] = std::max(maxs[i], other.maxs[i]);
        extreme += other.extreme;
        if (other.hasHot &&
            (!hasHot || hotter(other.hotTemp, other.hotTs, other.hotStation,
                               hotTemp, hotTs, hotStation))) {
            hotTemp = other.hotTemp; hotTs = other.hotTs;
            hotStation = other.hotStation; hasHot = true;
        }
        if (other.hasCold &&
            (!hasCold || colder(other.coldTemp, other.coldTs, other.coldStation,
                                coldTemp, coldTs, coldStation))) {
            coldTemp = other.coldTemp; coldTs = other.coldTs;
            coldStation = other.coldStation; hasCold = true;
        }
        for (const auto& kv : other.intervals) intervals[kv.first] += kv.second;
        for (const auto& kv : other.stations) {
            StationAgg& s = stations[kv.first];
            s.count += kv.second.count;
            s.tempSum += kv.second.tempSum;
            s.rainSum += kv.second.rainSum;
        }
    }

    // Serialize this partial as wire-format lines.
    void emit(std::ostream& out, bool withMeta, long long topK) const {
        std::string line;
        if (withMeta) {
            out << "meta\tK=" << topK << "\n";
        }
        if (count <= 0) return;
        line = "count\t" + std::to_string(count) + "\n";
        out << line;
        line = "sums\t";
        for (int i = 0; i < 5; ++i) { if (i) line += ' '; appendDouble(line, sums[i]); }
        line += '\n'; out << line;
        line = "mins\t";
        for (int i = 0; i < 3; ++i) { if (i) line += ' '; appendDouble(line, mins[i]); }
        line += '\n'; out << line;
        line = "maxs\t";
        for (int i = 0; i < 5; ++i) { if (i) line += ' '; appendDouble(line, maxs[i]); }
        line += '\n'; out << line;
        out << "extreme\t" << extreme << "\n";
        if (hasHot) {
            line = "hot\t"; appendDouble(line, hotTemp);
            line += ' ' + std::to_string(hotStation) + ' ' + std::to_string(hotTs) + "\n";
            out << line;
        }
        if (hasCold) {
            line = "cold\t"; appendDouble(line, coldTemp);
            line += ' ' + std::to_string(coldStation) + ' ' + std::to_string(coldTs) + "\n";
            out << line;
        }
        for (const auto& kv : intervals)
            out << "iv\t" << kv.first << ' ' << kv.second << "\n";
        for (const auto& kv : stations) {
            line = "st\t" + std::to_string(kv.first) + ' ' +
                   std::to_string(kv.second.count) + ' ';
            appendDouble(line, kv.second.tempSum); line += ' ';
            appendDouble(line, kv.second.rainSum); line += '\n';
            out << line;
        }
    }

    // Parse one wire-format line and MERGE it into this partial. Every tag
    // merges commutatively (counts/sums add, minima/min, maxima/max, hot/cold
    // compare-and-keep), so the order of arrival never matters.
    // Returns false if the tag is not recognized.
    bool absorb(const std::string& key, const std::string& value) {
        std::istringstream vs(value);
        if (key == "count") {
            long long n = 0;
            vs >> n;
            count += n;
            return true;
        }
        if (key == "sums") {
            for (int i = 0; i < 5; ++i) {
                std::string tok; vs >> tok;
                sums[i] += std::strtod(tok.c_str(), nullptr);
            }
            return true;
        }
        if (key == "mins") {
            for (int i = 0; i < 3; ++i) {
                std::string tok; vs >> tok;
                mins[i] = std::min(mins[i], std::strtod(tok.c_str(), nullptr));
            }
            return true;
        }
        if (key == "maxs") {
            for (int i = 0; i < 5; ++i) {
                std::string tok; vs >> tok;
                maxs[i] = std::max(maxs[i], std::strtod(tok.c_str(), nullptr));
            }
            return true;
        }
        if (key == "extreme") {
            long long n = 0;
            vs >> n;
            extreme += n;
            return true;
        }
        if (key == "hot") {
            std::string tok;
            double t; int st; long long ts;
            vs >> tok; t = std::strtod(tok.c_str(), nullptr);
            vs >> st >> ts;
            if (!hasHot || hotter(t, ts, st, hotTemp, hotTs, hotStation)) {
                hotTemp = t; hotStation = st; hotTs = ts; hasHot = true;
            }
            return true;
        }
        if (key == "cold") {
            std::string tok;
            double t; int st; long long ts;
            vs >> tok; t = std::strtod(tok.c_str(), nullptr);
            vs >> st >> ts;
            if (!hasCold || colder(t, ts, st, coldTemp, coldTs, coldStation)) {
                coldTemp = t; coldStation = st; coldTs = ts; hasCold = true;
            }
            return true;
        }
        if (key == "iv") {
            long long interval, n;
            vs >> interval >> n;
            intervals[interval] += n;
            return true;
        }
        if (key == "st") {
            long long station, n;
            std::string tsumTok, rsumTok;
            vs >> station >> n >> tsumTok >> rsumTok;
            StationAgg& s = stations[station];
            s.count += n;
            s.tempSum += std::strtod(tsumTok.c_str(), nullptr);
            s.rainSum += std::strtod(rsumTok.c_str(), nullptr);
            return true;
        }
        return false;
    }
};

// The 19-line report, identical to the baseline sequential/MPI output.
inline void printReport(std::ostream& out, const Partial& agg, long long topK) {
    std::ostringstream o;
    o.precision(6);
    o << std::fixed;
    const bool has = agg.count > 0;
    o << "TOTAL_MEASUREMENTS " << agg.count << "\n"
      << "AVERAGE_TEMPERATURE " << (has ? agg.sums[0] / agg.count : 0.0) << "\n"
      << "MIN_TEMPERATURE " << (has ? agg.mins[0] : 0.0) << "\n"
      << "MAX_TEMPERATURE " << (has ? agg.maxs[0] : 0.0) << "\n"
      << "AVERAGE_HUMIDITY " << (has ? agg.sums[1] / agg.count : 0.0) << "\n"
      << "MIN_HUMIDITY " << (has ? agg.mins[1] : 0.0) << "\n"
      << "MAX_HUMIDITY " << (has ? agg.maxs[1] : 0.0) << "\n"
      << "AVERAGE_PRESSURE " << (has ? agg.sums[2] / agg.count : 0.0) << "\n"
      << "MIN_PRESSURE " << (has ? agg.mins[2] : 0.0) << "\n"
      << "MAX_PRESSURE " << (has ? agg.maxs[2] : 0.0) << "\n"
      << "TOTAL_RAINFALL " << agg.sums[3] << "\n"
      << "MAX_RAINFALL " << (has ? agg.maxs[3] : 0.0) << "\n"
      << "AVERAGE_WIND_SPEED " << (has ? agg.sums[4] / agg.count : 0.0) << "\n"
      << "MAX_WIND_SPEED " << (has ? agg.maxs[4] : 0.0) << "\n"
      << "EXTREME_TEMPERATURE_EVENTS " << agg.extreme << "\n"
      << "HOTTEST_MEASUREMENT "
      << (has ? agg.hotTemp : 0.0) << ' '
      << (has ? agg.hotStation : 0) << ' '
      << (has ? agg.hotTs : 0) << "\n"
      << "COLDEST_MEASUREMENT "
      << (has ? agg.coldTemp : 0.0) << ' '
      << (has ? agg.coldStation : 0) << ' '
      << (has ? agg.coldTs : 0) << "\n";

    long long bestInterval = -1, bestCount = -1;
    for (const auto& kv : agg.intervals) {
        if (kv.second > bestCount ||
            (kv.second == bestCount && kv.first < bestInterval)) {
            bestCount = kv.second;
            bestInterval = kv.first;
        }
    }
    o << "BUSIEST_INTERVAL " << (has ? bestInterval : -1) << ' '
      << (has ? bestCount : 0) << "\n"
      << "TOP_STATIONS\n";

    std::vector<std::pair<long long, const StationAgg*>> stations;
    for (const auto& kv : agg.stations)
        if (kv.second.count > 0) stations.push_back({kv.first, &kv.second});
    std::sort(stations.begin(), stations.end(),
              [](const std::pair<long long, const StationAgg*>& a,
                 const std::pair<long long, const StationAgg*>& b) {
                  if (a.second->count != b.second->count)
                      return a.second->count > b.second->count;
                  return a.first < b.first;
              });
    if ((long long)stations.size() > topK) stations.resize(topK);
    for (const auto& entry : stations) {
        o << entry.first << ' ' << entry.second->count << ' '
          << entry.second->tempSum / entry.second->count << ' '
          << entry.second->rainSum << "\n";
    }
    out << o.str();
}

}  // namespace q8mr
