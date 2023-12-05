#include <nmmintrin.h>
#include <cstdlib>
#include <cstdint>

class BitMap {
public:
    BitMap() {}
    BitMap(size_t bytes) {
        kNumUINT64 = (bytes + 7) / 8;
        bits = (uint64_t*)calloc(kNumUINT64, sizeof(uint64_t));
    }
    ~BitMap() { free(bits); }

    size_t get_first_free_bit() {
        for (size_t i = 0; i < kNumUINT64; ++i) {
            if (~bits[i] == 0)
                continue;
            return i * 64 + __builtin_ctzll(~bits[i]);
        }
        return kNumUINT64 * 64;
    }

    bool is_free_bit() {
        return false;
    }

    bool Test(size_t p) const {
        return bits[p / 64] & (1ULL << (63 - p % 64));
    }

    void Set(size_t p) {
        bits[p / 64] |= (1ULL << (63 - p % 64));
    }

    void Clear(size_t p) {
        bits[p / 64] &= ~(1ULL << (63 - p % 64));
    }

    void ClearAll() {
        memset(bits, 0, kNumUINT64);
    }

    void SetAll() {
        memset(bits, 0xff, kNumUINT64);
    }

private:
    uint64_t* bits;
    size_t kNumUINT64;
};