from gather_clustering_counts.gather_release_counts import find_link


def test_find_links():
    d1 = {
        'A': ['1', '2'],
        'B': ['2', '5'],
        'C': ['3', '4'],
        'D': ['5'],
        'E': []
    }
    d2 = {
        '1': ['A', 'B'],
        '2': ['A'],
        '3': ['C'],
        '4': ['C'],
        '5': ['D', 'B']
    }
    assert find_link({'A'}, d1, d2) == (frozenset({'A', 'B', 'D'}), frozenset({'1', '2', '5'}))
    assert find_link({'B'}, d1, d2) == (frozenset({'A', 'B', 'D'}), frozenset({'1', '2', '5'}))
    assert find_link({'C'}, d1, d2) == (frozenset({'C'}), frozenset({'3', '4'}))
    assert find_link({'D'}, d1, d2) == (frozenset({'A', 'B', 'D'}), frozenset({'1', '2', '5'}))
    assert find_link({'E'}, d1, d2) == (frozenset({'E'}), frozenset({}))
