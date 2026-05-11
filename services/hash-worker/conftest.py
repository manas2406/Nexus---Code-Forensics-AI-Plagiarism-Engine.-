"""Pytest configuration and fixtures for Phase 2 tests.

Registers the ``integration`` marker and provides shared fixtures
for synthetic C++ plagiarism pairs.
"""

from __future__ import annotations

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as requiring live infra (MinIO + Redis)",
    )


@pytest.fixture
def sample_cpp_files() -> dict[str, str]:
    """10 synthetic C++ files — 2 plagiarism pairs + 6 originals.

    Uses the same generator style from benchmark.py to produce
    structurally identical pairs with renamed variables.
    """
    return {
        # ── Pair 1: original ──
        "pair1_orig.cpp": """\
int compute_0(int n) {
    int result = 0;
    for (int i = 0; i < n; i++) {
        result += i * i;
        if (i % 2 == 0) {
            result -= i;
        }
    }
    int final_val = result + n;
    for (int j = 0; j < n; j++) {
        final_val += j;
    }
    return final_val;
}
""",
        # ── Pair 1: clone (renamed vars) ──
        "pair1_clone.cpp": """\
int compute_0(int count) {
    int total = 0;
    for (int idx = 0; idx < count; idx++) {
        total += idx * idx;
        if (idx % 2 == 0) {
            total -= idx;
        }
    }
    int answer = total + count;
    for (int k = 0; k < count; k++) {
        answer += k;
    }
    return answer;
}
""",
        # ── Pair 2: original ──
        "pair2_orig.cpp": """\
int sort_array(int arr[], int size) {
    for (int i = 0; i < size - 1; i++) {
        for (int j = 0; j < size - i - 1; j++) {
            if (arr[j] > arr[j + 1]) {
                int temp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = temp;
            }
        }
    }
    return 0;
}
""",
        # ── Pair 2: clone (renamed vars) ──
        "pair2_clone.cpp": """\
int sort_array(int data[], int length) {
    for (int x = 0; x < length - 1; x++) {
        for (int y = 0; y < length - x - 1; y++) {
            if (data[y] > data[y + 1]) {
                int tmp = data[y];
                data[y] = data[y + 1];
                data[y + 1] = tmp;
            }
        }
    }
    return 0;
}
""",
        # ── Originals (structurally different) ──
        "orig_01.cpp": """\
#include <iostream>
int fibonacci(int n) {
    if (n <= 1) return n;
    int a = 0, b = 1;
    for (int i = 2; i <= n; i++) {
        int c = a + b; a = b; b = c;
    }
    return b;
}
""",
        "orig_02.cpp": """\
#include <cmath>
double distance(double x1, double y1, double x2, double y2) {
    return sqrt((x2 - x1) * (x2 - x1) + (y2 - y1) * (y2 - y1));
}
""",
        "orig_03.cpp": """\
class Stack {
public:
    int data[100];
    int top;
    Stack() : top(-1) {}
    void push(int val) { data[++top] = val; }
    int pop() { return data[top--]; }
    bool empty() { return top == -1; }
};
""",
        "orig_04.cpp": """\
#include <string>
int count_vowels(const std::string& s) {
    int count = 0;
    for (char c : s) {
        if (c == 'a' || c == 'e' || c == 'i' || c == 'o' || c == 'u') count++;
    }
    return count;
}
""",
        "orig_05.cpp": """\
int gcd(int a, int b) {
    while (b != 0) {
        int t = b;
        b = a % b;
        a = t;
    }
    return a;
}
""",
        "orig_06.cpp": """\
#include <vector>
std::vector<int> merge(const std::vector<int>& a, const std::vector<int>& b) {
    std::vector<int> result;
    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i] <= b[j]) result.push_back(a[i++]);
        else result.push_back(b[j++]);
    }
    while (i < a.size()) result.push_back(a[i++]);
    while (j < b.size()) result.push_back(b[j++]);
    return result;
}
""",
    }
