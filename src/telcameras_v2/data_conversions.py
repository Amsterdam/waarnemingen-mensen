def data_to_observation(data):
    """
    This endpoint receives data in a format which does not match the database layout. This function
    transforms its structure to match the database.
    """
    observation = data[0]
    message = observation.pop('message')
    version = observation.pop('version')
    counts = []
    persons = []
    for obs in data:
        if obs['message_type'] == 'count':
            for count in obs['aggregate']:
                count['external_id'] = count.pop(
                    'id')  # Count aggregates have an id, so to avoid colisions with the django orm id we rename the existing id that to "external_id"
                count['message'] = message
                count['version'] = version
                if 'geom' in count:
                    count['geom'] = None if not count['geom'] else count['geom']
                counts.append(count)
        elif obs['message_type'] == 'person':
            for person in obs['aggregate']:
                person['message'] = message
                person['version'] = version
                person['person_id'] = person.pop('personId')
                if 'geom' in person:
                    person['geom'] = None if not person['geom'] else person['geom']
                persons.append(person)

    observation['counts'] = counts
    observation['persons'] = persons

    del observation['message_type']
    del observation['aggregate']

    # Round lat/longs to 13 decimal places, because sometimes they are absurdly long
    observation['latitude'] = round(observation['latitude'], 13)
    observation['longitude'] = round(observation['longitude'], 13)

    return observation
