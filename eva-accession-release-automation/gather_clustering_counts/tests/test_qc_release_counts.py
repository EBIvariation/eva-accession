from gather_clustering_counts.qc_release_counts import compare_counts


def test_compare_counts():
    file_counts = {
        'tax_1': {
            'asm_1': {'current_rs': 1}
        },
        'tax_2': {
            'asm_2': {'current_rs': 3, 'multi_mapped_rs': 18, 'merged_rs': 5}
        }
    }
    db_counts = {
        'tax_2': {
            'asm_2': {'current_rs': 45, 'multi_mapped_rs': 1, 'merged_rs': 5},
            'asm_3': {'current_rs': 0, 'multi_mapped_rs': 2}
        }
    }

    # negative threshold shows all counts for all metrics (missing counts treated as 0)
    negative_threshold = compare_counts(file_counts, db_counts, -1)
    assert len(negative_threshold) == 15  # 5 metrics for each of 3 (tax, asm) pairs

    # threshold=0 shows only actual differences
    zero_threshold = compare_counts(file_counts, db_counts, 0)
    assert sorted(zero_threshold) == [
        ('tax_1', 'asm_1', 'current_rs', 1, 0, 1),
        ('tax_2', 'asm_2', 'current_rs', 3, 45, -42),
        ('tax_2', 'asm_2', 'multi_mapped_rs', 18, 1, 17),
        ('tax_2', 'asm_3', 'multi_mapped_rs', 0, 2, -2)
    ]

    # positive threshold shows differences greater than this in either direction
    positive_threshold = compare_counts(file_counts, db_counts, 10)
    assert sorted(positive_threshold) == [
        ('tax_2', 'asm_2', 'current_rs', 3, 45, -42),
        ('tax_2', 'asm_2', 'multi_mapped_rs', 18, 1, 17)
    ]
