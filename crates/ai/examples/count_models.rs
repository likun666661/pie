fn main() {
    let all = pie_ai::list_models();
    println!("Total: {}", all.len());
    let mut by_provider: std::collections::HashMap<String, usize> = Default::default();
    for m in &all {
        *by_provider.entry(m.provider.0.clone()).or_default() += 1;
    }
    let mut entries: Vec<_> = by_provider.into_iter().collect();
    entries.sort_by(|a, b| b.1.cmp(&a.1));
    for (p, n) in entries.iter().take(15) {
        println!("  {:30} {}", p, n);
    }
}
