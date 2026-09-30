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

struct MeasurementStats {
    long long measurementCount = 0;
    double temperatureSum = 0.0;
    double humiditySum = 0.0;
    double pressureSum = 0.0;
    double rainfallSum = 0.0;
    double windSpeedSum = 0.0;
    double minTemperature = numeric_limits<double>::infinity();
    double maxTemperature = -numeric_limits<double>::infinity();
    double minHumidity = numeric_limits<double>::infinity();
    double maxHumidity = -numeric_limits<double>::infinity();
    double minPressure = numeric_limits<double>::infinity();
    double maxPressure = -numeric_limits<double>::infinity();
    double maxRainfall = -numeric_limits<double>::infinity();
    double maxWindSpeed = -numeric_limits<double>::infinity();
    long long extremeTemperatureEvents = 0;

    void addMeasurement(double temperature, double humidity, double pressure, double rainfall,
                        double windSpeed) {
        ++measurementCount;
        temperatureSum += temperature;
        humiditySum += humidity;
        pressureSum += pressure;
        rainfallSum += rainfall;
        windSpeedSum += windSpeed;
        minTemperature = min(minTemperature, temperature);
        maxTemperature = max(maxTemperature, temperature);
        minHumidity = min(minHumidity, humidity);
        maxHumidity = max(maxHumidity, humidity);
        minPressure = min(minPressure, pressure);
        maxPressure = max(maxPressure, pressure);
        maxRainfall = max(maxRainfall, rainfall);
        maxWindSpeed = max(maxWindSpeed, windSpeed);
        if (temperature >= 40.0 || temperature <= 0.0) ++extremeTemperatureEvents;
    }
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

int main(int argc, char* argv[]) {
    try {
        InputReader reader(argc > 1 ? argv[1] : "");
        const long long measurementTotal = reader.nextLongLong();
        const long long topK = reader.nextLongLong();
        const long long stationTotal = reader.nextLongLong();

        MeasurementStats stats;
        TemperatureExtreme hottest;
        TemperatureExtreme coldest;
        hottest.temperature = -numeric_limits<double>::infinity();
        coldest.temperature = numeric_limits<double>::infinity();
        unordered_map<long long, long long> intervalCounts;
        vector<long long> stationMeasurementCounts(stationTotal > 0 ? stationTotal : 0, 0);
        vector<double> stationTemperatureSums(stationTotal > 0 ? stationTotal : 0, 0.0);
        vector<double> stationRainfallSums(stationTotal > 0 ? stationTotal : 0, 0.0);

        for (long long index = 0; index < measurementTotal; ++index) {
            const long long timestamp = reader.nextLongLong();
            const long long stationId = reader.nextLongLong();
            const double temperature = reader.nextDouble();
            const double humidity = reader.nextDouble();
            const double pressure = reader.nextDouble();
            const double rainfall = reader.nextDouble();
            const double windSpeed = reader.nextDouble();

            stats.addMeasurement(temperature, humidity, pressure, rainfall, windSpeed);

            TemperatureExtreme current{temperature, timestamp, static_cast<int>(stationId)};
            if (isHotter(current, hottest)) hottest = current;
            if (isColder(current, coldest)) coldest = current;

            ++intervalCounts[timestamp / 60];

            if (stationId >= 0 && stationId < stationTotal) {
                ++stationMeasurementCounts[stationId];
                stationTemperatureSums[stationId] += temperature;
                stationRainfallSums[stationId] += rainfall;
            }
        }

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
            if (stationMeasurementCounts[station] > 0) stationOrder.push_back(static_cast<int>(station));
        sort(stationOrder.begin(), stationOrder.end(), [&](int left, int right) {
            if (stationMeasurementCounts[left] != stationMeasurementCounts[right])
                return stationMeasurementCounts[left] > stationMeasurementCounts[right];
            return left < right;
        });
        if (static_cast<long long>(stationOrder.size()) > topK) stationOrder.resize(topK);

        ostringstream output;
        output << fixed << setprecision(6);
        const bool hasMeasurements = stats.measurementCount > 0;
        output << "TOTAL_MEASUREMENTS " << stats.measurementCount << "\n"
               << "AVERAGE_TEMPERATURE "
               << (hasMeasurements ? stats.temperatureSum / stats.measurementCount : 0.0) << "\n"
               << "MIN_TEMPERATURE " << (hasMeasurements ? stats.minTemperature : 0.0) << "\n"
               << "MAX_TEMPERATURE " << (hasMeasurements ? stats.maxTemperature : 0.0) << "\n"
               << "AVERAGE_HUMIDITY "
               << (hasMeasurements ? stats.humiditySum / stats.measurementCount : 0.0) << "\n"
               << "MIN_HUMIDITY " << (hasMeasurements ? stats.minHumidity : 0.0) << "\n"
               << "MAX_HUMIDITY " << (hasMeasurements ? stats.maxHumidity : 0.0) << "\n"
               << "AVERAGE_PRESSURE "
               << (hasMeasurements ? stats.pressureSum / stats.measurementCount : 0.0) << "\n"
               << "MIN_PRESSURE " << (hasMeasurements ? stats.minPressure : 0.0) << "\n"
               << "MAX_PRESSURE " << (hasMeasurements ? stats.maxPressure : 0.0) << "\n"
               << "TOTAL_RAINFALL " << stats.rainfallSum << "\n"
               << "MAX_RAINFALL " << (hasMeasurements ? stats.maxRainfall : 0.0) << "\n"
               << "AVERAGE_WIND_SPEED "
               << (hasMeasurements ? stats.windSpeedSum / stats.measurementCount : 0.0) << "\n"
               << "MAX_WIND_SPEED " << (hasMeasurements ? stats.maxWindSpeed : 0.0) << "\n"
               << "EXTREME_TEMPERATURE_EVENTS " << stats.extremeTemperatureEvents << "\n"
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
            output << station << ' ' << stationMeasurementCounts[station] << ' '
                   << stationTemperatureSums[station] / stationMeasurementCounts[station] << ' '
                   << stationRainfallSums[station] << "\n";
        }
        cout << output.str();
    } catch (const exception& error) {
        cerr << error.what() << "\n";
        return 1;
    }
    return 0;
}
