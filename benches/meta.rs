use divan::counter::BytesCount;
use noatun::data_types::noatun_hash_map::{
    get_any_empty, run_get_probe_sequence, BucketProbeSequence, Meta, MetaGroup, MetaGroupNr,
};
use std::hint::black_box;

fn main() {
    // Run registered benchmarks.
    divan::main();
}

const SIZES: [usize; 4] = [1usize, 2, 8, 1024];

#[divan::bench(consts = SIZES, skip_ext_time)]
fn meta<const N: usize>(bencher: divan::Bencher) {
    let mut haystack = [MetaGroup([Meta::new(129u8); 32]); N];
    haystack[N - 1].0[13] = Meta::new(142u8);
    let needle = Meta::new(142u8);

    let probe = BucketProbeSequence::new(MetaGroupNr(0), N);
    bencher
        .counter(BytesCount::new(32 * N as u64))
        .bench_local(move || {
            run_get_probe_sequence(
                &haystack,
                32,
                needle,
                |pos| {
                    black_box(pos);
                    false
                },
                probe,
            );
        });
}

#[divan::bench(skip_ext_time)]
fn find_zeroes(bencher: divan::Bencher) {
    let mut group = MetaGroup([Meta::new(129u8); 32]);
    group.0[13] = Meta::new(142u8);

    bencher
        .counter(BytesCount::new(32u64))
        .bench_local(move || {
            black_box(get_any_empty(black_box(&group)));
        });
}
