import logging

from aviso.settings import sec_context
from pymongo import InsertOne, ReturnDocument, UpdateOne
from pymongo.errors import BulkWriteError, DuplicateKeyError

from infra.constants import (AUDIT_COLL, DEALS_COLL, DRILLDOWN_COLL,
                   DRILLDOWN_LEADS_COLL, HIER_COLL, HIER_LEADS_COLL)
from infra.read import (fetch_ancestors, fetch_boundry,
                        fetch_closest_boundaries, fetch_descendant_ids,
                        fetch_hidden_nodes, fetch_prev_boundaries)
from utils.date_utils import (epoch, get_prev_eod, next_period_by_epoch,
                              prev_period_by_epoch)
from utils.misc_utils import (BootstrapError, CycleError, is_lead_service,
                              iter_chunks)

logger = logging.getLogger('aviso-core.%s' % __name__)


def _set_run_full_mode_flag(value):
    sec_context.details.set_flag(DEALS_COLL, 'run_full_mode', value)

def create_node(parent,
                node,
                label,
                as_of,
                config,
                signature='create_node',
                drilldown=False,
                as_to=None,
                is_team=False,
                service=None):
    """
    create a single node

    Arguments:
        parent {str} -- parent node                                             '00509005WEMDAY7'
        node {str} -- node                                                      '0050000FLN2C9I2'
        label {str} -- human readable node name                                 'EMEA'
        as_of {int} -- epoch timestamp for action to count as of                1556074024910
        config {HierConfig} -- config for hierarchy, from HierConfig            ...?

    Keyword Arguments:
        signature {str} -- what process triggered action
                           (default: {'create_node'})
        drilldown {bool} -- if True, write to drilldown collection
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = sec_context.tenant_db[coll]

    # something about guaranteeing rep node is a crm owner id
    # TODO: no cycles, guarantee node is not already in collection. its guaranteed in api layer...

    record = {'from': as_of if config.versioned_hierarchy else None,
              'to': as_to,
              'how': signature,
              'parent': parent,
              'node': node,
              'label': label}

    criteria = {'node': node,
                'parent': parent,
                'from': as_of if config.versioned_hierarchy else None,
                'to': as_to
                }
    if is_team:
        record['is_team'] = True
    hier_collection.update_one(criteria, {'$set': record}, upsert=True)
    #Enable flag to run the capturedrilldowntask
    if not is_lead_service(service):
        _set_run_full_mode_flag(True)


def update_results_using_query(query, update_fields, drilldown=False):
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    hier_collection = sec_context.tenant_db[coll]
    hier_collection.update_many(query, {"$set": update_fields})
    return True


def create_many_nodes(node_to_parent,
                      labels,
                      as_of,
                      config,
                      signature='bootstrap_nodes',
                      drilldown=False,
                      as_to=None,
                      period_type='Q',
                      service=None
                      ):
    """
    initialize a hierarchy from scratch, can only be done once per tenant

    Arguments:
        node_to_parent {dict} -- mapping of node to parent node                 {'0050000FLN2C9I2': '00509005WEMDAY7'}
        labels {str} -- mapping of node to label                                {'0050000FLN2C9I2': 'EMEA'}
        as_of {int} -- epoch timestamp for action to count as of                1556074024910
        config {HierConfig} -- config for hierarchy, from HierConfig            ...?

    Keyword Arguments:
        signature {str} -- what process triggered action
                           (default: {'bootstrao_nodes'})
        drilldown {bool} -- if True, write to drilldown collection
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one

    Raises:
        BootstrapError: will throw an error if tenant has already been bootstrapped
    """
    if not drilldown and config.bootstrapped:
        raise BootstrapError('hierarchy has already been bootstrapped')
    elif drilldown and config.bootstrapped_drilldown:
        raise BootstrapError('drilldown has already been bootstrapped')

    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = sec_context.tenant_db[coll]
    is_end_date_none = False
    prev_from_timestamp, prev_to_timestamp, next_from_timestamp, next_to_timestamp = None, None, None, None

    # expiring current hierarchy
    if config.versioned_hierarchy:

        prev_from_timestamp, prev_to_timestamp, next_from_timestamp, next_to_timestamp = fetch_closest_boundaries(as_of)

        # This is a common query that needs to be executed when there a version already exists betweent he given as_of
        # and as_to
        hier_collection.delete_many({'$and': [{'from': {'$gte': as_of}}, {'to': {'$lte': as_to}}]})

        # Check for the conditions only when there is a version that starts before/on the given as_of
        if prev_from_timestamp is not None:
            if prev_from_timestamp <= as_of and prev_to_timestamp is None:
                # The cases that fall under this are:
                # 1. Quarter is first uploaded, Jan 1st to March 31st, Feb/March version is uploaded. Consider a new version
                # is uploaded in March,
                # now the old version is cut off from Jan 1st to Feb 28th and new version starts from March 1st to None
                hier_collection.update_many(
                    {'$and': [{'from': {'$eq': prev_from_timestamp}}, {'to': {'$eq': None}}]},
                    {'$set': {'to': epoch(prev_period_by_epoch(as_of, period_type).end).as_epoch()}})

            elif prev_from_timestamp < as_of and (prev_to_timestamp is not None and prev_to_timestamp == as_to):
                # The cases that fall under this are:
                # 1. There exists a version from Jan 1st to Feb 28th and another one from March 1st to 31st, now a new
                # version is uploaded for the month of Feb, now the earlier version is cut off from Jan 1st to Jan 31st and
                # a new version is uploaded for Feb 1st to Feb 28th.
                hier_collection.update_many(
                    {'$and': [{'from': {'$eq': prev_from_timestamp}}, {'to': {'$eq': prev_to_timestamp}}]},
                    {'$set': {'to': epoch(prev_period_by_epoch(as_of, period_type).end).as_epoch()}})

            elif prev_from_timestamp == as_of and (prev_to_timestamp is not None and prev_to_timestamp > as_to):
                # The cases that fall under this are:
                # 1. There exists a version from Jan 1st to Feb 28th and March 1st to March 31st, now a new version is
                # uploaded for the month of Jan, then the prev version is cutoff from Feb 1st to Feb 28th and a new
                # version is uploaded for Jan
                hier_collection.update_many(
                    {'$and': [{'from': {'$eq': prev_from_timestamp}}, {'to': {'$eq': prev_to_timestamp}}]},
                    {'$set': {'from': epoch(next_period_by_epoch(as_of, period_type).begin).as_epoch()}})


        # Check for the conditions only when there exists another version which starts on/after the given as_of
        # and the prev_from_timestamp is not equal to next_from_timestamp as those scenarios would have already been
        # handled above
        if next_from_timestamp is not None and next_from_timestamp != prev_from_timestamp:
            if next_to_timestamp is not None and next_from_timestamp >= as_of \
            and next_from_timestamp < as_to and next_to_timestamp > as_to:

                hier_collection.update_many(
                    {'$and': [{'from': {'$eq': next_from_timestamp}}, {'to': {'$eq': next_to_timestamp}}]},
                    {'$set': {'from': epoch(next_period_by_epoch(as_of, period_type).begin).as_epoch()}})

            elif next_from_timestamp >= as_of and next_from_timestamp < as_to and next_to_timestamp is None:

                hier_collection.delete_many({'$and': [{'from': {'$gte': as_of}}, {'to': None}]})

        # This is to make sure that the last version carries on to the next quarters as well.
        # The if elif is intentional
        is_end_date_none = False
        if next_from_timestamp is None and prev_from_timestamp is None:
            is_end_date_none = True
        elif next_from_timestamp is not None and next_to_timestamp is None:
            is_end_date_none = True
        elif prev_from_timestamp is not None and prev_to_timestamp is None:
            is_end_date_none = True
    else:
        hier_collection.delete_many({})
        as_of = None
        as_to = None
        is_end_date_none = True

    updates = []
    for node, parent in node_to_parent.items():
        record = {'from': as_of,
                  'to': as_to if not is_end_date_none else None,
                  'how': signature,
                  'parent': parent,
                  'node': node,
                  'label': labels.get(node, node)}

        updates.append(UpdateOne({'node': node,
                                  'parent': parent,
                                  'from': as_of,
                                  'to': as_to},
                                 {'$set': record},
                                 upsert=True))  # TODO: this should be an insert, right??


    bulk_write(updates, hier_collection)
    # Enable flag to run the capturedrilldowntask
    if not is_lead_service(service):
        _set_run_full_mode_flag(True)

        bootstrap_flag = 'bootstrapped' if not drilldown else 'bootstrapped_drilldown'
        config.update_config({bootstrap_flag: True})


def move_node(parent,
              node,
              as_of,
              config,
              signature='move_node',
              ignore_cycles=False,
              drilldown=False,
              as_to=None,
              service=None
            ):
    """
    move node to become a child of new parent

    Arguments:
        parent {str} -- parent node                                             '00509005WEMDAY7'
        node {str} -- node                                                      '0050000FLN2C9I2'
        as_of {int} -- epoch timestamp for action to count as of                1556074024910
        config {HierConfig} -- config for hierarchy, from HierConfig            ...?

    Keyword Arguments:
        signature {str} -- what process triggered action
                           (default: {'move_node'})
        drilldown {bool} -- if True, write to drilldown collection
                            (default: {False})


    Raises:
        CycleError: will throw error if node move would create a cycle in hierarchy
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = sec_context.tenant_db[coll]

    if not ignore_cycles and _cycle_check(node, parent, as_of, drilldown, service=service):
        raise CycleError('moving %s to report to %s would create a cycle', node, parent)

    if config.versioned_hierarchy:
        # This is to make sure that the boundry for the move rep is correctly set based on the given as_of
        as_of,as_to = fetch_boundry(as_of, drilldown, service=service)

        # expire old node record
        delete_criteria = {'node': node,
                           'from': as_of,
                           'to': as_to}
        node_rec = hier_collection.find_one_and_delete(delete_criteria,
                                                       projection={'_id': 0},
                                                       )

        # insert new one
        record = {'from': as_of,
                  'to': as_to,
                  'how': signature,
                  'parent': parent,
                  'node': node,
                  'label': node_rec['label'],
                  }
        hier_collection.insert_one(record)
    else:
        # TODO: is it really guaranteed that theres only one record with node: node ?
        record = hier_collection.find_one_and_update({'node': node,
                                                      'to': as_to}, {'$set': {'parent': parent,
                                                                             'how': signature}},
                                                     projection={'_id': 0},
                                                     return_document=ReturnDocument.AFTER)
        if config.hierarchy_builder == 'collab_fcst':
            # unhide the hidden child nodes as the parent is moved and not hidden anymore, if there is any child which
            # has to be hidden, next sync will taken care of it
            hidden_nodes = fetch_hidden_nodes(as_of, drilldown=drilldown)
            hidden_nodes = [rec['node'] for rec in hidden_nodes]
            descendants = fetch_descendant_ids(as_of, node, drilldown=drilldown, include_hidden=True)
            for node in descendants:
                if node in hidden_nodes:
                    unhide_node(node,
                                as_of,
                                config,
                                signature=signature)


    # Enable flag to run the capturedrilldowntask
    if not is_lead_service(service):
        _set_run_full_mode_flag(True)

    # audit = {'service': 'hier_svc',
    #          'coll': coll,
    #          'timestamp': epoch().as_epoch(),
    #          'user': sec_context.login_user_name,
    #          'record': record}
    # audit_collection.insert_one(audit)


def hide_node(node,
              as_of,
              config,
              signature='hide_node',
              hide_descendants=True,
              drilldown=False,
              as_to=None,
              service=None
              ):
    """
    hide node from hierarchy

    Arguments:
        node {str} -- node                                                      '0050000FLN2C9I2'
        config {HierConfig} -- config for hierarchy, from HierConfig            ...?
        as_of {int} -- epoch timestamp for action to count as of                1556074024910

    Keyword Arguments:
        signature {str} -- what process triggered action
                           (default: {'hide_node'})
        hide_descendants {bool} -- hide descendants of node
                                   (default: {True})
        drilldown {bool} -- if True, write to drilldown collection
                            (default: {False})

    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = sec_context.tenant_db[coll]
    audit_collection = sec_context.tenant_db[AUDIT_COLL]
    user = sec_context.login_user_name
    timestamp = epoch().as_epoch()
    as_of = as_of if config.versioned_hierarchy else 0

    descendants = fetch_descendant_ids(as_of, node, drilldown=drilldown, service=service)

    # TODO: for versioned hierarchies, is hiding in past versions of hierarchy a requirement?
    # hide node
    audit = hier_collection.find_one_and_update({'node': node,
                                                 'to': as_to}, {'$set': {'hidden_by': None,
                                                                        'hidden_from': as_of,
                                                                        'how': signature},
                                                               '$unset': {"hidden_to":1}},
                                                projection={'_id': 0},
                                                return_document=ReturnDocument.AFTER)

    # hide living descendants of node
    if hide_descendants:
        hier_collection.update_many({'node': {'$in': descendants},
                                     'hidden': {'$ne': True},
                                     'to': as_to}, {'$set': {'hidden_by': node,
                                                            'hidden_from': as_of,
                                                            'how': signature},
                                                   '$unset': {"hidden_to": 1}})

        audits = [InsertOne({'user': user,
                             'service': 'hier_svc',
                             'timestamp': timestamp,
                             'coll': coll,
                             'record': node})
                  for node in hier_collection.find({'node': {'$in': descendants},
                                                    'to': as_to,
                                                    'hidden_by': node},
                                                   {'_id': 0})]
    else:
        audits = []

    if not is_lead_service(service):
        audits.append(InsertOne({'user': user,
                                 'service': 'hier_svc',
                                 'timestamp': timestamp,
                                 'coll': coll,
                                 'record': audit}))

        bulk_write(audits, audit_collection)
        # Enable flag to run the capturedrilldowntask
        _set_run_full_mode_flag(True)

    return audit


def unhide_node(node,
                as_of,
                config,
                signature='unhide_node',
                drilldown=False,
                as_to=None,
                service=None
                ):
    """
    unhide node from hierarchy

    Arguments:
        node {str} -- node                                                      '0050000FLN2C9I2'
        config {HierConfig} -- config for hierarchy, from HierConfig            ...?
        as_of {int} -- epoch timestamp for action to count as of                1556074024910

    Keyword Arguments:
        signature {str} -- what process triggered action
                           (default: {'unhide_node'})
        drilldown {bool} -- if True, write to drilldown collection
                            (default: {False})
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = sec_context.tenant_db[coll]
    audit_collection = sec_context.tenant_db[AUDIT_COLL]
    user = sec_context.login_user_name
    timestamp = epoch().as_epoch()
    as_of = as_of if config.versioned_hierarchy else 0

    descendants = fetch_descendant_ids(as_of, node, include_hidden=True, drilldown=drilldown, service=service)

    # unhide node
    audit = hier_collection.find_one_and_update({'node': node,
                                                 'to': as_to}, {'$set': {'hidden_by': None,
                                                                        'hidden_to': as_of,
                                                                        'how': signature}},
                                                projection={'_id': 0},
                                                return_document=ReturnDocument.AFTER)

    # unhide descendants of node that were hidden when that node was hidden
    hier_collection.update_many({'node': {'$in': descendants},
                                 'hidden_by': node,
                                 'to': as_to}, {'$set': {'hidden_by': None,
                                                        'hidden_to': as_of,
                                                        'how': signature}})

    audits = [InsertOne({'user': user,
                         'service': 'hier_svc',
                         'audit_timestamp': timestamp,
                         'coll': coll,
                         'record': node})
              for node in hier_collection.find({'node': {'$in': descendants},
                                                'to': as_to,
                                                'hidden_by': None},
                                               {'_id': 0})]
    audits.append(InsertOne({'user': user,
                             'service': 'hier_svc',
                             'audit_timestamp': timestamp,
                             'coll': coll,
                             'record': audit}))

    if not is_lead_service(service):
        bulk_write(audits, audit_collection)
        # Enable flag to run the capturedrilldowntask
        _set_run_full_mode_flag(True)


def label_node(node,
               label,
               config,
               signature='label_node',
               drilldown=False,
               as_to=None,
               service=None
               ):
    """
    give node a human readable label

    Arguments:
        node {str} -- node                                                      '0050000FLN2C9I2'
        label {str} -- human readable node name                                 'EMEA'
        config {HierConfig} -- config for hierarchy, from HierConfig            ...?

    Keyword Arguments:
        signature {str} -- what process triggered action
                           (default: {'label_node'})
        drilldown {bool} -- if True, write to drilldown collection
                            (default: {False})
    """
    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = sec_context.tenant_db[coll]

    hier_collection.update({'node': node,
                            'to': as_to}, {'$set': {'label': label,
                                                   'how': signature}})
    # Enable flag to run the capturedrilldowntask
    if not is_lead_service(service):
        _set_run_full_mode_flag(True)

def update_nodes_with_valid_to_timestamp(from_timestamp,
                                        drilldown=False,
                                         service=None):
    """
    Update "to" with a valid closed timestamp

    Arguments:
        from_timestamp {epoch} -- from timestamp to consider the closing of boundaries       1556074024910

    Keyword Arguments:
        drilldown {bool} -- if True, write to drilldown collection
                            (default: {False})
        db {object} -- instance of tenant_db (default: {None})
                       if None, will create one
    """

    coll = HIER_COLL if not drilldown else DRILLDOWN_COLL
    if is_lead_service(service):
        coll = HIER_LEADS_COLL if not drilldown else DRILLDOWN_LEADS_COLL
    hier_collection = sec_context.tenant_db[coll]

    next_mnemonic_from_timestamp = from_timestamp

    for boundary_node in fetch_prev_boundaries(from_timestamp, drilldown, service=service):
        if not boundary_node["to"]:
            prev_eod = get_prev_eod(epoch(next_mnemonic_from_timestamp))

            hier_collection.update_many(
                    {'$and': [{'from': {'$eq': boundary_node["from"]}}, {'to': {'$eq': None}}]},
                    {'$set': {'to': prev_eod.as_epoch()}})

            next_mnemonic_from_timestamp = boundary_node["from"]


def _cycle_check(node, parent, as_of, drilldown, service=None):
    try:
        parent_ancestors = fetch_ancestors(as_of, [parent], include_hidden=True, drilldown=drilldown, service=service).next()['ancestors']
    except StopIteration:
        parent_ancestors = set()
    return node in parent_ancestors


def bulk_write(operations,
               coll,
               chunk_size=1000,
               attempts=3,
               success_threshold=0,
               stats=None,
               session=None):
    """
    write many records to a collection at once

    Arguments:
        operations {list} -- list of mongo db operations to perform
                             [InsertOne({'hello': 'world'}), ..., UpdateOne({'idx': 'idx'}, {'$set': {'x': 1}})]
        coll {pymongo.collection.Collection} -- mongo db collection to write to

    Keyword Arguments:
        chunk_size {int} -- how many records to attempt to bulk write in one go (default: {1000})
        attempts {int} -- how many tries to rewrite records if failure occurs (default: {3})
        success_threshold {int} -- how many failed record writes are acceptable (default: {0})

    Returns:
        bool -- success of write
    """
    failed_writes = []
    while operations and attempts:
        for batch in iter_chunks(operations, chunk_size):
            try:
                result = coll.bulk_write(batch, ordered=False, bypass_document_validation=True, session=session)
                if stats:
                    stats['inserted_count'] += result.inserted_count
                    stats['modified_count'] += result.modified_count
            except BulkWriteError as bwe:
                logger.error(bwe.details)
                bad_idx = bwe.details['writeErrors'][0]['index']
                failed_writes.extend(batch[bad_idx:])
            except DuplicateKeyError as e:
                logger.exception("DuplicateKeyError for {}".format(e))
            except Exception as e:
                logger.error(str(e))
        operations = failed_writes
        chunk_size /= 2
        attempts -= 1
    return len(failed_writes) <= success_threshold
