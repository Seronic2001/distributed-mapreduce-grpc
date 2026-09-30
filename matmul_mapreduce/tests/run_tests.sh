#!/bin/bash
# ============================================================
# Correctness tests for the Row-Row MapReduce matrix multiplication.
#
#  * The two examples from the problem spec, both formats,
#    including the even (3 mappers, 1 row each) and uneven
#    (3 mappers, 2+1+1 rows) splits.
#  * Edge cases: m = 1 (single row of A), n = 1 (single column
#    of A / single row of B), more mappers than rows (R > m).
#  * Randomised square / tall / wide / sparse matrices against
#    the brute-force oracle verify.py, with and without combiner.
# ============================================================
set -u
cd "$(dirname "$0")/.."
TMP=/tmp/opencode/s1q1
mkdir -p "$TMP"
fail=0
pass=0

run_and_check() {
    local name="$1" a="$2" b="$3" expected="$4" R="$5" combine="$6"
    local out="$TMP/out.txt"
    if [ "$combine" = "1" ]; then
        COMBINE=1 bash run_local.sh "$a" "$b" "$out" "$R" 2>/dev/null
    else
        COMBINE=0 bash run_local.sh "$a" "$b" "$out" "$R" 2>/dev/null
    fi
    if [ $? -ne 0 ]; then
        echo "$name: RUN FAIL"; fail=1; return
    fi
    if diff -q "$expected" "$out" >/dev/null 2>&1; then
        echo "$name: PASS"; pass=$((pass+1))
    else
        echo "$name: FAIL"
        diff "$expected" "$out" | head -8
        fail=1
    fi
}

# ---------- Example 1 (even split, 3 mappers, 1 row each) ----------
cat > "$TMP/A1.txt" <<'EOF'
1 2
0 3
-1 4
EOF
cat > "$TMP/B1.txt" <<'EOF'
2 3 4
1 0 -1
EOF
cat > "$TMP/C1_expected.txt" <<'EOF'
4 3 2
3 0 -3
2 -3 -8
EOF
run_and_check "pdf_example1_even_R3" "$TMP/A1.txt" "$TMP/B1.txt" "$TMP/C1_expected.txt" 3 1
run_and_check "pdf_example1_even_R3_nocombiner" "$TMP/A1.txt" "$TMP/B1.txt" "$TMP/C1_expected.txt" 3 0

# Same example in sparse format (split unevenly by line count).
# Expected C comes from the brute-force oracle in sparse format.
cat > "$TMP/A1s.txt" <<'EOF'
0	0	1
0	1	2
1	1	3
2	0	-1
2	1	4
EOF
cat > "$TMP/B1s.txt" <<'EOF'
0	0	2
0	1	3
0	2	4
1	0	1
1	2	-1
EOF
python3 verify.py "$TMP/A1s.txt" "$TMP/B1s.txt" --sparse > "$TMP/C1s_expected.txt"
run_and_check "pdf_example1_sparse_R2" "$TMP/A1s.txt" "$TMP/B1s.txt" "$TMP/C1s_expected.txt" 2 1
run_and_check "pdf_example1_sparse_R5" "$TMP/A1s.txt" "$TMP/B1s.txt" "$TMP/C1s_expected.txt" 5 1

# ---------- Example 2 (uneven split: mapper1 2 rows, mappers 2,3: 1 row) ----------
cat > "$TMP/A2.txt" <<'EOF'
1 0
2 -1
0 3
1 1
EOF
cat > "$TMP/B2.txt" <<'EOF'
1 2
0 1
EOF
cat > "$TMP/C2_expected.txt" <<'EOF'
1 2
2 3
0 3
1 3
EOF
run_and_check "pdf_example2_uneven_R3" "$TMP/A2.txt" "$TMP/B2.txt" "$TMP/C2_expected.txt" 3 1
run_and_check "pdf_example2_uneven_R3_nocombiner" "$TMP/A2.txt" "$TMP/B2.txt" "$TMP/C2_expected.txt" 3 0
run_and_check "pdf_example2_uneven_R1" "$TMP/A2.txt" "$TMP/B2.txt" "$TMP/C2_expected.txt" 1 1

# ---------- Edge cases ----------
# m = 1: single row of A (1 x 3) times B (3 x 4)
printf '3 1 4\n' > "$TMP/A_m1.txt"
printf '1 0 -1 2\n0 2 0 1\n2 1 0 3\n' > "$TMP/B_m1.txt"
# 3*b0 + 1*b1 + 4*b2 = (3+0+8, 0+2+4, -3+0+0, 6+1+12) = (11, 6, -3, 19)
printf '11 6 -3 19\n' > "$TMP/C_m1_expected.txt"
run_and_check "edge_m1" "$TMP/A_m1.txt" "$TMP/B_m1.txt" "$TMP/C_m1_expected.txt" 4 1

# n = 1: single column of A (3 x 1) times single row of B (1 x 4)
printf '5\n-2\n0\n' > "$TMP/A_n1.txt"
printf '2 0 -1 4\n' > "$TMP/B_n1.txt"
printf '10 0 -5 20\n-4 0 2 -8\n0 0 0 0\n' > "$TMP/C_n1_expected.txt"
run_and_check "edge_n1" "$TMP/A_n1.txt" "$TMP/B_n1.txt" "$TMP/C_n1_expected.txt" 3 1

printf -- '-2\n' > "$TMP/A_m1n1.txt"                   # m = n = p = 1
printf -- '3\n' > "$TMP/B_single.txt"
printf -- '-6\n' > "$TMP/C_single_expected.txt"
run_and_check "edge_1x1" "$TMP/A_m1n1.txt" "$TMP/B_single.txt" "$TMP/C_single_expected.txt" 2 1

# ---------- Randomised tests vs the brute-force oracle ----------
i=0
for cfg in "3 2 3 1.0"  "4 4 4 1.0"  "5 2 7 1.0"  "10 10 10 0.4" \
           "7 1 5 1.0"  "1 6 1 1.0"  "50 40 30 0.2"  "16 16 16 0.05"; do
    i=$((i+1))
    set -- $cfg
    python3 gen_matrices.py --m "$1" --n "$2" --p "$3" --density "$4" \
        --seed $((100 + i)) --out-a "$TMP/ra.txt" --out-b "$TMP/rb.txt"
    python3 verify.py "$TMP/ra.txt" "$TMP/rb.txt" > "$TMP/rexpected.txt"
    R=$(( (i % 4) + 1 ))
    run_and_check "random_$i(m=$1,n=$2,p=$3,d=$4,R=$R)" \
        "$TMP/ra.txt" "$TMP/rb.txt" "$TMP/rexpected.txt" "$R" 1
    run_and_check "random_${i}_nocombiner" \
        "$TMP/ra.txt" "$TMP/rb.txt" "$TMP/rexpected.txt" "$R" 0
done

# ---------- Sparse randomised tests (ijv format end to end) ----------
for seed in 7 8 9; do
    python3 gen_matrices.py --m 20 --n 15 --p 12 --density 0.15 --seed "$seed" \
        --sparse --out-a "$TMP/sa.txt" --out-b "$TMP/sb.txt"
    python3 verify.py "$TMP/sa.txt" "$TMP/sb.txt" --sparse > "$TMP/sexpected.txt"
    run_and_check "sparse_seed$seed" "$TMP/sa.txt" "$TMP/sb.txt" "$TMP/sexpected.txt" 3 1
done

echo "----------------------------------------"
echo "$pass passed, $fail failed"
exit $fail
