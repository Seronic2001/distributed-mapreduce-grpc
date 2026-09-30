#include <mpi.h>
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

using namespace std;

class InputReader {
public:
    explicit InputReader(const string& path) {
        if (path.empty()) {
            char buffer[1 << 20];
            while (cin) {
                cin.read(buffer, sizeof(buffer));
                data.insert(data.end(), buffer, buffer + cin.gcount());
            }
        } else {
            ifstream file(path, ios::binary);
            if (!file) throw runtime_error("cannot open " + path);
            file.seekg(0, ios::end);
            streamsize size = file.tellg();
            file.seekg(0, ios::beg);
            data.resize(static_cast<size_t>(size));
            file.read(data.data(), size);
        }
    }

    long long nextLongLong() {
        skipUntilTokenStart();
        bool negative = false;
        if (peek() == '-') {
            negative = true;
            advance();
        }
        long long value = 0;
        while (onDigit()) {
            value = value * 10 + (peek() - '0');
            advance();
        }
        return negative ? -value : value;
    }

    double nextDouble() {
        skipUntilTokenStart();
        bool negative = false;
        if (peek() == '-') {
            negative = true;
            advance();
        }
        double value = 0.0;
        while (onDigit()) {
            value = value * 10.0 + (peek() - '0');
            advance();
        }
        if (position < data.size() && peek() == '.') {
            advance();
            double fraction = 1.0;
            while (onDigit()) {
                fraction /= 10.0;
                value += (peek() - '0') * fraction;
                advance();
            }
        }
        return negative ? -value : value;
    }

private:
    vector<char> data;
    size_t position = 0;

    char peek() const { return data[position]; }
    void advance() { ++position; }
    bool onDigit() const {
        return position < data.size() && data[position] >= '0' && data[position] <= '9';
    }
    void skipUntilTokenStart() {
        while (position < data.size() && !isTokenStart(data[position])) advance();
    }
    static bool isTokenStart(char c) { return (c >= '0' && c <= '9') || c == '-' || c == '.'; }
};

struct WeatherRecord {
    double temperature;
    double humidity;
    double pressure;
    double rainfall;
    double windSpeed;
    long long timestamp;
    int stationId;
};

struct TemperatureExtreme {
    double temperature = 0.0;
    long long timestamp = 0;
    int stationId = 0;
};

bool isHotter(const TemperatureExtreme& candidate, const TemperatureExtreme& incumbent) {
    if (candidate.temperature != incumbent.temperature)
        return candidate.temperature > incumbent.temperature;
    if (candidate.timestamp != incumbent.timestamp)
        return candidate.timestamp < incumbent.timestamp;
    return candidate.stationId < incumbent.stationId;
}

bool isColder(const TemperatureExtreme& candidate, const TemperatureExtreme& incumbent) {
    if (candidate.temperature != incumbent.temperature)
        return candidate.temperature < incumbent.temperature;
    if (candidate.timestamp != incumbent.timestamp)
        return candidate.timestamp < incumbent.timestamp;
    return candidate.stationId < incumbent.stationId;
}

enum MetricIndex { TEMPERATURE = 0, HUMIDITY, PRESSURE, RAINFALL, WIND_SPEED, METRIC_COUNT };

int main(int argc, char* argv[]) {
    MPI_Init(&argc, &argv);
    int rank = 0;
    int processCount = 0;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &processCount);

    const double startTime = MPI_Wtime();
    double distributeTime = 0.0;
    double computeTime = 0.0;
    double collectTime = 0.0;

    long long measurementTotal = 0;
    long long topK = 0;
    long long stationTotal = 0;
    vector<WeatherRecord> allRecords;

    if (rank == 0) {
        try {
            InputReader reader(argc > 1 ? argv[1] : "");
            measurementTotal = reader.nextLongLong();
            topK = reader.nextLongLong();
            stationTotal = reader.nextLongLong();
            allRecords.resize(measurementTotal);
            for (WeatherRecord& record : allRecords) {
                record.timestamp = reader.nextLongLong();
                record.stationId = static_cast<int>(reader.nextLongLong());
                record.temperature = reader.nextDouble();
                record.humidity = reader.nextDouble();
                record.pressure = reader.nextDouble();
                record.rainfall = reader.nextDouble();
                record.windSpeed = reader.nextDouble();
            }
        } catch (const exception& error) {
            cerr << error.what() << "\n";
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    vector<long long> header = {measurementTotal, topK, stationTotal};
    MPI_Bcast(header.data(), 3, MPI_LONG_LONG, 0, MPI_COMM_WORLD);
    measurementTotal = header[0];
    topK = header[1];
    stationTotal = header[2];

    vector<int> recordsPerProcess(processCount);
    vector<int> recordDisplacements(processCount);
    vector<int> byteCounts(processCount);
    vector<int> byteDisplacements(processCount);
    for (int process = 0; process < processCount; ++process) {
        const long long count =
            measurementTotal / processCount + (process < measurementTotal % processCount ? 1 : 0);
        recordsPerProcess[process] = static_cast<int>(count);
        recordDisplacements[process] =
            (process == 0) ? 0 : recordDisplacements[process - 1] + recordsPerProcess[process - 1];
        byteCounts[process] = recordsPerProcess[process] * static_cast<int>(sizeof(WeatherRecord));
        byteDisplacements[process] =
            recordDisplacements[process] * static_cast<int>(sizeof(WeatherRecord));
    }

    vector<WeatherRecord> localRecords(recordsPerProcess[rank]);

    if (rank == 0) distributeTime = MPI_Wtime();
    MPI_Scatterv(rank == 0 ? allRecords.data() : nullptr, byteCounts.data(),
                 byteDisplacements.data(), MPI_BYTE, localRecords.data(),
                 recordsPerProcess[rank] * static_cast<int>(sizeof(WeatherRecord)), MPI_BYTE, 0,
                 MPI_COMM_WORLD);
    if (rank == 0) {
        distributeTime = MPI_Wtime() - distributeTime;
        computeTime = MPI_Wtime();
    }

    const double positiveInfinity = numeric_limits<double>::infinity();
    const double negativeInfinity = -numeric_limits<double>::infinity();

    vector<double> localSums(METRIC_COUNT, 0.0);
    vector<double> localMinima(3, positiveInfinity);
    vector<double> localMaxima(METRIC_COUNT, negativeInfinity);
    vector<long long> localCounters(2, 0);
    TemperatureExtreme localHottest;
    localHottest.temperature = negativeInfinity;
    TemperatureExtreme localColdest;
    localColdest.temperature = positiveInfinity;
    unordered_map<long long, long long> localIntervalCounts;
    vector<long long> stationMeasurementCounts(stationTotal > 0 ? stationTotal : 0, 0);
    vector<double> stationTemperatureSums(stationTotal > 0 ? stationTotal : 0, 0.0);
    vector<double> stationRainfallSums(stationTotal > 0 ? stationTotal : 0, 0.0);

    for (const WeatherRecord& record : localRecords) {
        localSums[TEMPERATURE] += record.temperature;
        localSums[HUMIDITY] += record.humidity;
        localSums[PRESSURE] += record.pressure;
        localSums[RAINFALL] += record.rainfall;
        localSums[WIND_SPEED] += record.windSpeed;
        localMinima[TEMPERATURE] = min(localMinima[TEMPERATURE], record.temperature);
        localMinima[HUMIDITY] = min(localMinima[HUMIDITY], record.humidity);
        localMinima[PRESSURE] = min(localMinima[PRESSURE], record.pressure);
        localMaxima[TEMPERATURE] = max(localMaxima[TEMPERATURE], record.temperature);
        localMaxima[HUMIDITY] = max(localMaxima[HUMIDITY], record.humidity);
        localMaxima[PRESSURE] = max(localMaxima[PRESSURE], record.pressure);
        localMaxima[RAINFALL] = max(localMaxima[RAINFALL], record.rainfall);
        localMaxima[WIND_SPEED] = max(localMaxima[WIND_SPEED], record.windSpeed);
        ++localCounters[0];
        if (record.temperature >= 40.0 || record.temperature <= 0.0) ++localCounters[1];

        TemperatureExtreme current{record.temperature, record.timestamp, record.stationId};
        if (isHotter(current, localHottest)) localHottest = current;
        if (isColder(current, localColdest)) localColdest = current;

        ++localIntervalCounts[record.timestamp / 60];

        if (record.stationId >= 0 && record.stationId < stationTotal) {
            ++stationMeasurementCounts[record.stationId];
            stationTemperatureSums[record.stationId] += record.temperature;
            stationRainfallSums[record.stationId] += record.rainfall;
        }
    }

    if (rank == 0) {
        computeTime = MPI_Wtime() - computeTime;
        collectTime = MPI_Wtime();
    }

    vector<double> globalSums(METRIC_COUNT);
    vector<double> globalMinima(3);
    vector<double> globalMaxima(METRIC_COUNT);
    vector<long long> globalCounters(2);
    MPI_Reduce(localSums.data(), globalSums.data(), METRIC_COUNT, MPI_DOUBLE, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(localCounters.data(), globalCounters.data(), 2, MPI_LONG_LONG, MPI_SUM, 0,
               MPI_COMM_WORLD);
    MPI_Reduce(localMinima.data(), globalMinima.data(), 3, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(localMaxima.data(), globalMaxima.data(), METRIC_COUNT, MPI_DOUBLE, MPI_MAX, 0,
               MPI_COMM_WORLD);

    vector<TemperatureExtreme> hottestCandidates(processCount);
    vector<TemperatureExtreme> coldestCandidates(processCount);
    MPI_Gather(&localHottest, static_cast<int>(sizeof(TemperatureExtreme)), MPI_BYTE,
               hottestCandidates.data(), static_cast<int>(sizeof(TemperatureExtreme)), MPI_BYTE, 0,
               MPI_COMM_WORLD);
    MPI_Gather(&localColdest, static_cast<int>(sizeof(TemperatureExtreme)), MPI_BYTE,
               coldestCandidates.data(), static_cast<int>(sizeof(TemperatureExtreme)), MPI_BYTE, 0,
               MPI_COMM_WORLD);

    vector<long long> intervalKeys;
    vector<long long> intervalValues;
    intervalKeys.reserve(localIntervalCounts.size());
    intervalValues.reserve(localIntervalCounts.size());
    for (const auto& [interval, count] : localIntervalCounts) {
        intervalKeys.push_back(interval);
        intervalValues.push_back(count);
    }
    const int localIntervalCount = static_cast<int>(intervalKeys.size());
    vector<int> intervalCountsPerProcess(processCount);
    vector<int> intervalDisplacements(processCount);
    MPI_Gather(&localIntervalCount, 1, MPI_INT, intervalCountsPerProcess.data(), 1, MPI_INT, 0,
               MPI_COMM_WORLD);
    if (rank == 0) {
        intervalDisplacements[0] = 0;
        for (int process = 1; process < processCount; ++process)
            intervalDisplacements[process] =
                intervalDisplacements[process - 1] + intervalCountsPerProcess[process - 1];
    }
    const int totalIntervals =
        (rank == 0) ? intervalDisplacements[processCount - 1] + intervalCountsPerProcess[processCount - 1]
                    : 0;
    vector<long long> mergedIntervalKeys(totalIntervals);
    vector<long long> mergedIntervalValues(totalIntervals);
    MPI_Gatherv(intervalKeys.data(), localIntervalCount, MPI_LONG_LONG, mergedIntervalKeys.data(),
                rank == 0 ? intervalCountsPerProcess.data() : nullptr,
                rank == 0 ? intervalDisplacements.data() : nullptr, MPI_LONG_LONG, 0,
                MPI_COMM_WORLD);
    MPI_Gatherv(intervalValues.data(), localIntervalCount, MPI_LONG_LONG,
                mergedIntervalValues.data(),
                rank == 0 ? intervalCountsPerProcess.data() : nullptr,
                rank == 0 ? intervalDisplacements.data() : nullptr, MPI_LONG_LONG, 0,
                MPI_COMM_WORLD);

    vector<long long> globalStationCounts(stationTotal > 0 ? stationTotal : 0);
    vector<double> globalStationTemperatureSums(stationTotal > 0 ? stationTotal : 0);
    vector<double> globalStationRainfallSums(stationTotal > 0 ? stationTotal : 0);
    MPI_Reduce(stationMeasurementCounts.data(), globalStationCounts.data(),
               static_cast<int>(stationTotal), MPI_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(stationTemperatureSums.data(), globalStationTemperatureSums.data(),
               static_cast<int>(stationTotal), MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(stationRainfallSums.data(), globalStationRainfallSums.data(),
               static_cast<int>(stationTotal), MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        TemperatureExtreme hottest;
        hottest.temperature = negativeInfinity;
        TemperatureExtreme coldest;
        coldest.temperature = positiveInfinity;
        for (int process = 0; process < processCount; ++process) {
            const TemperatureExtreme& hotCandidate = hottestCandidates[process];
            const TemperatureExtreme& coldCandidate = coldestCandidates[process];
            if (hotCandidate.temperature != negativeInfinity && isHotter(hotCandidate, hottest))
                hottest = hotCandidate;
            if (coldCandidate.temperature != positiveInfinity && isColder(coldCandidate, coldest))
                coldest = coldCandidate;
        }

        unordered_map<long long, long long> intervalCounts;
        for (int index = 0; index < totalIntervals; ++index)
            intervalCounts[mergedIntervalKeys[index]] += mergedIntervalValues[index];
        long long busiestInterval = -1;
        long long busiestCount = -1;
        for (const auto& [interval, count] : intervalCounts) {
            if (count > busiestCount || (count == busiestCount && interval < busiestInterval)) {
                busiestCount = count;
                busiestInterval = interval;
            }
        }

        vector<int> stationOrder;
        for (long long station = 0; station < stationTotal; ++station)
            if (globalStationCounts[station] > 0)
                stationOrder.push_back(static_cast<int>(station));
        sort(stationOrder.begin(), stationOrder.end(), [&](int left, int right) {
            if (globalStationCounts[left] != globalStationCounts[right])
                return globalStationCounts[left] > globalStationCounts[right];
            return left < right;
        });
        if (static_cast<long long>(stationOrder.size()) > topK) stationOrder.resize(topK);

        const long long measurementCount = globalCounters[0];
        const bool hasMeasurements = measurementCount > 0;
        ostringstream output;
        output << fixed << setprecision(6);
        output << "TOTAL_MEASUREMENTS " << measurementCount << "\n"
               << "AVERAGE_TEMPERATURE "
               << (hasMeasurements ? globalSums[TEMPERATURE] / measurementCount : 0.0) << "\n"
               << "MIN_TEMPERATURE " << (hasMeasurements ? globalMinima[TEMPERATURE] : 0.0) << "\n"
               << "MAX_TEMPERATURE " << (hasMeasurements ? globalMaxima[TEMPERATURE] : 0.0) << "\n"
               << "AVERAGE_HUMIDITY "
               << (hasMeasurements ? globalSums[HUMIDITY] / measurementCount : 0.0) << "\n"
               << "MIN_HUMIDITY " << (hasMeasurements ? globalMinima[HUMIDITY] : 0.0) << "\n"
               << "MAX_HUMIDITY " << (hasMeasurements ? globalMaxima[HUMIDITY] : 0.0) << "\n"
               << "AVERAGE_PRESSURE "
               << (hasMeasurements ? globalSums[PRESSURE] / measurementCount : 0.0) << "\n"
               << "MIN_PRESSURE " << (hasMeasurements ? globalMinima[PRESSURE] : 0.0) << "\n"
               << "MAX_PRESSURE " << (hasMeasurements ? globalMaxima[PRESSURE] : 0.0) << "\n"
               << "TOTAL_RAINFALL " << globalSums[RAINFALL] << "\n"
               << "MAX_RAINFALL " << (hasMeasurements ? globalMaxima[RAINFALL] : 0.0) << "\n"
               << "AVERAGE_WIND_SPEED "
               << (hasMeasurements ? globalSums[WIND_SPEED] / measurementCount : 0.0) << "\n"
               << "MAX_WIND_SPEED " << (hasMeasurements ? globalMaxima[WIND_SPEED] : 0.0) << "\n"
               << "EXTREME_TEMPERATURE_EVENTS " << globalCounters[1] << "\n"
               << "HOTTEST_MEASUREMENT "
               << (hasMeasurements ? hottest.temperature : 0.0) << ' '
               << (hasMeasurements ? hottest.stationId : 0) << ' '
               << (hasMeasurements ? hottest.timestamp : 0) << "\n"
               << "COLDEST_MEASUREMENT "
               << (hasMeasurements ? coldest.temperature : 0.0) << ' '
               << (hasMeasurements ? coldest.stationId : 0) << ' '
               << (hasMeasurements ? coldest.timestamp : 0) << "\n"
               << "BUSIEST_INTERVAL " << (hasMeasurements ? busiestInterval : -1) << ' '
               << (hasMeasurements ? busiestCount : 0) << "\n"
               << "TOP_STATIONS\n";
        for (const int station : stationOrder) {
            output << station << ' ' << globalStationCounts[station] << ' '
                   << globalStationTemperatureSums[station] / globalStationCounts[station] << ' '
                   << globalStationRainfallSums[station] << "\n";
        }
        cout << output.str();

        collectTime = MPI_Wtime() - collectTime;
        const double totalTime = MPI_Wtime() - startTime;
        cerr << fixed << setprecision(6)
             << "TIME_TOTAL " << totalTime << "\n"
             << "TIME_DISTRIBUTE " << distributeTime << "\n"
             << "TIME_COMPUTE " << computeTime << "\n"
             << "TIME_COLLECT " << collectTime << "\n";
    }

    MPI_Finalize();
    return 0;
}
