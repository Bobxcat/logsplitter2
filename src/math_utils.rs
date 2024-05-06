/// Returns a vector of `buckets` elements, which all add up to `sum`
///
/// Each element will be one of two values, which are a distance of `1` apart
pub fn get_even_partition(buckets: usize, sum: usize) -> Vec<usize> {
    let mut v = vec![sum / buckets; buckets];
    for i in 0..(sum % buckets) {
        v[i] += 1;
    }
    assert_eq!(v.iter().sum::<usize>(), sum);
    assert_eq!(v.len(), buckets);

    v
}
