


def entity_to_labels(entity):
    if isinstance(entity, type):
        name = entity.__name__
    else:
        name = entity.__class__.__name__

    parts = name.split('_')

    return normalize_labels(*parts)


def normalize_labels(*labels):
    if not labels:
        return ''

    labels = list(labels)
    labels.sort()

    return ':'.join(labels)


def entity_name(entity):
    if isinstance(entity, type):
        return '{}.{}'.format(entity.__module__, entity.__name__)
    else:
        return '{}.{}'.format(entity.__class__.__module__,
            entity.__class__.__name__)
