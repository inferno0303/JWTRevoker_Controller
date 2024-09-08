import math


def calculate_probability(k, n, m):
    p = (1 - math.exp(-k * n / m)) ** k
    return p


def optimal_k(m, n):
    if n == 0:
        return 0  # 防止除以零
    return (m / n) * math.log(2)


def calculate_m(n, p):
    if p == 0 or p >= 1:
        raise ValueError("p must be between 0 and 1 (exclusive)")
    m = - (n * math.log(p)) / (math.log(2) ** 2)
    return math.ceil(m)  # 向上取整，因为 m 必须是整数


def upscale_m(m):
    if m <= 0 or m > 8589934592:
        raise ValueError("m must be > 0 and <= 8589934592")
    m_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
              524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912,
              1073741824, 2147483648, 4294967296, 8589934592]
    for i in m_list:
        if i > m:
            return i


def searching(n, p_target):
    if p_target == 0 or p_target >= 1:
        raise ValueError("p must be between 0 and 1 (exclusive)")
    m_list = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072, 262144,
              524288, 1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864, 134217728, 268435456, 536870912,
              1073741824, 2147483648, 4294967296, 8589934592]
    for _m in m_list:
        for _k in range(100):
            _p = calculate_probability(_k, n, _m)
            if _p <= p_target:
                return _m, _k, _p
    return None, None, None


def main():
    n = 10000
    p_target = 0.001
    m_t = calculate_m(n, p_target)
    print(f"理论的位数组长度m={m_t}，即{m_t / 8192:.3f} KBytes")
    k_t = optimal_k(m_t, n)
    print(f"理论的哈希函数个数k={k_t:.3f}")
    p_t = calculate_probability(k_t, n, m_t)
    print(f"理论的布隆过滤器误判率p={p_t}，{'满足预设误判率' if p_t < p_target else '不满足预设误判率'}")
    k_t_round = round(k_t)
    print(f"将k取整后，k={k_t_round}，误判率p={calculate_probability(k_t_round, n, m_t)}，不满足预设误判率")

    print()
    m_upscale = upscale_m(m_t)
    print(f"将m向上取整为2的幂后，位数组长度m={m_upscale}，{m_upscale / 8192:.3f} KBytes")
    k_t_with_m_up = optimal_k(m_upscale, n)
    print(f"将m向上取整为2的幂后，理论的哈希函数个数k={k_t_with_m_up:.3f}")
    p_t_with_upm = calculate_probability(k_t_with_m_up, n, m_upscale)
    print(
        f"理论的布隆过滤器误判率p={p_t_with_upm:.9f}，{'满足预设误判率' if p_t_with_upm < p_target else '不满足预设误判率'}")
    k_t_with_m_up_round = round(k_t_with_m_up)
    print(f"将k取整后，k={k_t_with_m_up_round}，误判率p={calculate_probability(k_t_with_m_up_round, n, m_upscale):.9f}")

    print()
    m_s, k_s, p_s = searching(n, p_target)
    print(f"暴力搜索结果：m={m_s}, k={k_s}, p={p_s}")

if __name__ == '__main__':
    main()
