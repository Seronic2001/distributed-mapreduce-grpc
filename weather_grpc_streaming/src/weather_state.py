"""
Mergeable weather analytics state (Section 2 Q2).

This is the single source of truth for the analytics algebra, shared by the
coordinator, the workers, the dashboard renderer, and the correctness
reference:

  * ``WeatherPartial`` holds one node's aggregate. ``update_record`` folds in
    one streaming record; ``merge`` folds in another partial.
  * Both operations are **commutative and associative** — the same algebra
    the MapReduce job uses in weather_mapreduce/src/common.h — so any
    distribution of records over any number of workers produces exactly the
    same final analytics as a sequential pass.
  * Tie-breaking (identical to the baseline specification):
      hottest/coldest : temperature first, then smaller timestamp, then
                        smaller station id
      busiest interval: larger count, then smaller interval id
      top-K stations  : larger count, then smaller station id

Proto conversion helpers translate between WeatherPartial and the wire
Partial message so worker state can travel over gRPC.
"""

import weather_pb2

METRIC_COUNT = 5   # temperature humidity pressure rainfall wind_speed
MIN_METRIC_COUNT = 3
POS_INF = float("inf")
NEG_INF = float("-inf")


def _hotter(t, ts, st, t2, ts2, st2):
    if t != t2:
        return t > t2
    if ts != ts2:
        return ts < ts2
    return st < st2


def _colder(t, ts, st, t2, ts2, st2):
    if t != t2:
        return t < t2
    if ts != ts2:
        return ts < ts2
    return st < st2


class WeatherPartial:
    """One worker's (or the global) aggregate; mergeable in any order."""

    __slots__ = ("count", "sums", "mins", "maxs", "extreme", "hot", "cold",
                 "intervals", "stations")

    def __init__(self):
        self.count = 0
        self.sums = [0.0] * METRIC_COUNT
        self.mins = [POS_INF] * MIN_METRIC_COUNT
        self.maxs = [NEG_INF] * METRIC_COUNT
        self.extreme = 0
        # hottest/coldest: (temperature, timestamp, station_id)
        self.hot = None
        self.cold = None
        self.intervals = {}                     # interval -> count
        self.stations = {}                      # station -> [n, tsum, rsum]

    # ------------------------------------------------- streaming update --
    def update_record(self, ts, station, t, h, p, r, w):
        self.count += 1
        s = self.sums
        s[0] += t; s[1] += h; s[2] += p; s[3] += r; s[4] += w
        if t < self.mins[0]: self.mins[0] = t
        if h < self.mins[1]: self.mins[1] = h
        if p < self.mins[2]: self.mins[2] = p
        if t > self.maxs[0]: self.maxs[0] = t
        if h > self.maxs[1]: self.maxs[1] = h
        if p > self.maxs[2]: self.maxs[2] = p
        if r > self.maxs[3]: self.maxs[3] = r
        if w > self.maxs[4]: self.maxs[4] = w
        if t >= 40.0 or t <= 0.0:
            self.extreme += 1
        if self.hot is None or _hotter(t, ts, station, *self.hot):
            self.hot = (t, ts, station)
        if self.cold is None or _colder(t, ts, station, *self.cold):
            self.cold = (t, ts, station)
        interval = ts // 60
        self.intervals[interval] = self.intervals.get(interval, 0) + 1
        st = self.stations.get(station)
        if st is None:
            self.stations[station] = [1, t, r]
        else:
            st[0] += 1; st[1] += t; st[2] += r

    # -------------------------------------------------------- merging --
    def merge(self, other):
        self.count += other.count
        for i in range(METRIC_COUNT):
            self.sums[i] += other.sums[i]
        for i in range(MIN_METRIC_COUNT):
            if other.mins[i] < self.mins[i]:
                self.mins[i] = other.mins[i]
        for i in range(METRIC_COUNT):
            if other.maxs[i] > self.maxs[i]:
                self.maxs[i] = other.maxs[i]
        self.extreme += other.extreme
        if other.hot is not None and \
                (self.hot is None or _hotter(*other.hot, *self.hot)):
            self.hot = other.hot
        if other.cold is not None and \
                (self.cold is None or _colder(*other.cold, *self.cold)):
            self.cold = other.cold
        for interval, n in other.intervals.items():
            self.intervals[interval] = self.intervals.get(interval, 0) + n
        for station, (n, tsum, rsum) in other.stations.items():
            st = self.stations.get(station)
            if st is None:
                self.stations[station] = [n, tsum, rsum]
            else:
                st[0] += n; st[1] += tsum; st[2] += rsum

    # -------------------------------------------------- proto conversion --
    def to_proto(self, out=None):
        message = out if out is not None else weather_pb2.Partial()
        message.count = self.count
        message.sums.extend(self.sums)
        message.mins.extend(self.mins)
        message.maxs.extend(self.maxs)
        message.extreme_events = self.extreme
        if self.hot is not None:
            message.has_hottest = True
            message.hottest_temp, message.hottest_timestamp, \
                message.hottest_station = self.hot
        if self.cold is not None:
            message.has_coldest = True
            message.coldest_temp, message.coldest_timestamp, \
                message.coldest_station = self.cold
        for interval in sorted(self.intervals):
            message.intervals.add(interval=interval,
                                  count=self.intervals[interval])
        for station in sorted(self.stations):
            n, tsum, rsum = self.stations[station]
            message.stations.add(station_id=station, count=n,
                                 temperature_sum=tsum, rainfall_sum=rsum)
        return message

    @classmethod
    def from_proto(cls, message):
        partial = cls()
        partial.count = message.count
        partial.sums = list(message.sums) or [0.0] * METRIC_COUNT
        partial.mins = list(message.mins) or [POS_INF] * MIN_METRIC_COUNT
        partial.maxs = list(message.maxs) or [NEG_INF] * METRIC_COUNT
        partial.extreme = message.extreme_events
        if message.has_hottest:
            partial.hot = (message.hottest_temp, message.hottest_timestamp,
                           message.hottest_station)
        if message.has_coldest:
            partial.cold = (message.coldest_temp, message.coldest_timestamp,
                            message.coldest_station)
        for interval in message.intervals:
            partial.intervals[interval.interval] = \
                partial.intervals.get(interval.interval, 0) + interval.count
        for station in message.stations:
            st = partial.stations.get(station.station_id)
            if st is None:
                partial.stations[station.station_id] = [
                    station.count, station.temperature_sum,
                    station.rainfall_sum]
            else:
                st[0] += station.count
                st[1] += station.temperature_sum
                st[2] += station.rainfall_sum
        return partial

    # ---------------------------------------------------------- report --
    def busiest_interval(self):
        best_interval, best_count = -1, -1
        for interval, n in self.intervals.items():
            if n > best_count or (n == best_count and interval < best_interval):
                best_interval, best_count = interval, n
        return (best_interval, best_count) if self.count > 0 else (-1, 0)

    def top_stations(self, k):
        stations = [(sid, agg) for sid, agg in self.stations.items()
                    if agg[0] > 0]
        stations.sort(key=lambda item: (-item[1][0], item[0]))
        return stations[:k]

    def render(self, top_k):
        """The 19-line baseline report (identical to sequential/MPI/MR)."""
        has = self.count > 0
        if has:
            avg_t = self.sums[0] / self.count
            avg_h = self.sums[1] / self.count
            avg_p = self.sums[2] / self.count
            avg_w = self.sums[4] / self.count
            hot_t, hot_ts, hot_st = self.hot
            cold_t, cold_ts, cold_st = self.cold
            best_interval, best_count = self.busiest_interval()
        else:
            avg_t = avg_h = avg_p = avg_w = 0.0
            hot_t = hot_ts = hot_st = 0
            cold_t = cold_ts = cold_st = 0
            best_interval, best_count = -1, 0
        lines = [
            "TOTAL_MEASUREMENTS %d" % self.count,
            "AVERAGE_TEMPERATURE %.6f" % (avg_t if has else 0.0),
            "MIN_TEMPERATURE %.6f" % (self.mins[0] if has else 0.0),
            "MAX_TEMPERATURE %.6f" % (self.maxs[0] if has else 0.0),
            "AVERAGE_HUMIDITY %.6f" % (avg_h if has else 0.0),
            "MIN_HUMIDITY %.6f" % (self.mins[1] if has else 0.0),
            "MAX_HUMIDITY %.6f" % (self.maxs[1] if has else 0.0),
            "AVERAGE_PRESSURE %.6f" % (avg_p if has else 0.0),
            "MIN_PRESSURE %.6f" % (self.mins[2] if has else 0.0),
            "MAX_PRESSURE %.6f" % (self.maxs[2] if has else 0.0),
            "TOTAL_RAINFALL %.6f" % self.sums[3],
            "MAX_RAINFALL %.6f" % (self.maxs[3] if has else 0.0),
            "AVERAGE_WIND_SPEED %.6f" % (avg_w if has else 0.0),
            "MAX_WIND_SPEED %.6f" % (self.maxs[4] if has else 0.0),
            "EXTREME_TEMPERATURE_EVENTS %d" % self.extreme,
            "HOTTEST_MEASUREMENT %.6f %d %d" % (hot_t if has else 0.0,
                                                hot_st if has else 0,
                                                hot_ts if has else 0),
            "COLDEST_MEASUREMENT %.6f %d %d" % (cold_t if has else 0.0,
                                                cold_st if has else 0,
                                                cold_ts if has else 0),
            "BUSIEST_INTERVAL %d %d" % (best_interval, best_count),
            "TOP_STATIONS",
        ]
        for sid, (n, tsum, rsum) in self.top_stations(top_k):
            lines.append("%d %d %.6f %.6f" % (sid, n, tsum / n, rsum))
        return "\n".join(lines) + "\n"
