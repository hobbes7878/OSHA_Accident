def jaccard(v1, v2):
    """
    Jaccard takes two vectors and computes a score based on the number of items
    that overlap. Specifically, it is defined as the number of items contained
    in both sets (the intersection) divided by the total number of items in both
    sets combined (the union).
    
    This metric can be useful for calculating things like string similarity. A
    variation on this metric, described on its Wikipedia page, is especially helpful
    for measuring binary "market basket" similarity.
    """
    intersection = list(set(v1) & set(v2))
    union = list(set(v1) | set(v2))
    # Subtracting from 1.0 converts the measure into a distance
    return 1.0 - float(len(intersection)) / float(len(union))