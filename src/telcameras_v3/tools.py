from random import randint


def scramble_count(group_aggregate):
    """
    For privacy reasons we need a count which is slightly scrambled. We therefore add or subtract 1, or do nothing.
    """

    # Since this is not meant to be cryptographically secure we simply use the random module
    if group_aggregate.count is not None and group_aggregate.count_scrambled is None:
        group_aggregate.count_scrambled = group_aggregate.count + randint(-1, 1)
        if group_aggregate.count_scrambled < 0:
            group_aggregate.count_scrambled = 0

    return group_aggregate
