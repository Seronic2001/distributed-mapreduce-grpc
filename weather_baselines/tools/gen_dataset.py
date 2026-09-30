#!/usr/bin/env python3
import argparse
import random


def main():
    ap = argparse.ArgumentParser(
        description="Generate a weather dataset: 'timestamp station_id temperature humidity "
                    "pressure rainfall wind_speed' records with a fixed seed.")
    ap.add_argument("--n", type=int, required=True, help="number of measurements")
    ap.add_argument("--k", type=int, default=5, help="K for Top-K stations")
    ap.add_argument("--s", type=int, required=True, help="number of distinct stations")
    ap.add_argument("--seed", type=int, default=42, help="PRNG seed (reproducible)")
    ap.add_argument("--tmax", type=int, default=86400, help="max timestamp")
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    S = max(1, args.s)

    lines = [f"{args.n} {args.k} {S}"]
    for _ in range(args.n):
        ts = rng.randint(0, args.tmax)
        sid = rng.randrange(S)
        temp = rng.randint(-60, 100) * 0.5          # [-30.0, 50.0], 0.5 grid
        hum = rng.randint(0, 200) * 0.5             # [0.0, 100.0]
        press = rng.randint(1900, 2100) * 0.5       # [950.0, 1050.0]
        rain = rng.randint(0, 100) * 0.5            # [0.0, 50.0]
        wind = rng.randint(0, 80) * 0.5             # [0.0, 40.0]
        lines.append(f"{ts} {sid} {temp:.1f} {hum:.1f} {press:.1f} {rain:.1f} {wind:.1f}")

    text = "\n".join(lines) + "\n"
    if args.out:
        with open(args.out, "w") as f:
            f.write(text)
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
