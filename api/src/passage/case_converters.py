import re

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')

def to_snakecase(camel_str):
    s1 = first_cap_re.sub(r'\1_\2', camel_str)
    return all_cap_re.sub(r'\1_\2', s1).lower()

def to_camelcase(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])