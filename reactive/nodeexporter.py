from charms.reactive import when
from charmhelpers.core import hookenv


@when('prometheus-client.available')
def prometheus_client(prometheus):
    prometheus.configure(port=9100)

    principal_unit = get_principal_unit()
    prometheus.set_remote(**{'principal-unit': principal_unit})


def get_principal_unit():
    '''Return the principal unit for this subordinate.'''
    for relation_id in hookenv.relation_ids('container'):
        for relation_data in hookenv.relations_for_id(relation_id):
            return relation_data['__unit__']
