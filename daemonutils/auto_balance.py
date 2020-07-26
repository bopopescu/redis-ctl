import logging

from models.base import db
import models.node
import models.task
import models.audit


def _deploy_node(pod, aof, host, app):
    depl = app.container_client.deploy_redis(pod, aof, 'macvlan', host=host)
    cid = depl['container_id']
    h = depl['address']
    models.node.create_eru_instance(h, 6379, cid)
    return cid, h


def _rm_containers(cids, app):
    app.container_client.rm_containers(cids)
    for c in cids:
        try:
            models.node.delete_eru_instance(c)
        except ValueError as e:
            logging.exception(e)


def _prepare_main_node(node, pod, aof, host, app):
    cid, new_node_host = _deploy_node(pod, aof, host, app)
    try:
        task = models.task.ClusterTask(
            cluster_id=node.assignee_id,
            task_type=models.task.TASK_TYPE_AUTO_BALANCE,
            user_id=app.default_user_id())
        db.session.add(task)
        db.session.flush()
        logging.info(
            'Node deployed: container id=%s host=%s; joining cluster %d'
            ' [create task %d] use host %s',
            cid, new_node_host, node.assignee_id, task.id, host)
        task.add_step(
            'join', cluster_id=node.assignee_id,
            cluster_host=node.host, cluster_port=node.port,
            newin_host=new_node_host, newin_port=6379)
        return task, cid, new_node_host
    except BaseException as exc:
        logging.exception(exc)
        logging.info('Remove container %s and rollback', cid)
        _rm_containers([cid], app)
        db.session.rollback()
        raise


def _add_subordinates(subordinates, task, cluster_id, main_host, pod, aof, app):
    cids = []
    hosts = []
    try:
        for s in subordinates:
            logging.info('Auto deploy subordinate for main %s [task %d],'
                         ' use host %s', main_host, task.id, s.get('host'))
            cid, new_host = _deploy_node(pod, aof, s.get('host'), app)
            cids.append(cid)
            hosts.append(new_host)
            task.add_step('replicate', cluster_id=cluster_id,
                          main_host=main_host, main_port=6379,
                          subordinate_host=new_host, subordinate_port=6379)
        return cids, hosts
    except BaseException as exc:
        logging.info('Remove container %s and rollback', cids)
        _rm_containers(cids, app)
        db.session.rollback()
        raise


def add_node_to_balance_for(host, port, plan, slots, app):
    node = models.node.get_by_host_port(host, int(port))
    if node is None or node.assignee_id is None:
        logging.info(
            'No node or cluster found for %s:%d (This should be a corrupt)',
            host, port)
        return
    if node.assignee.current_task is not None:
        logging.info(
            'Fail to auto balance cluster %d for node %s:%d : busy',
            node.assignee_id, host, port)
        return

    task, cid, new_host = _prepare_main_node(
        node, plan.pod, plan.aof, plan.host, app)
    cids = [cid]
    hosts = [new_host]
    try:
        cs, hs = _add_subordinates(
            plan.subordinates, task, node.assignee_id,
            new_host, plan.pod, plan.aof, app)
        cids.extend(cs)
        hosts.extend(hs)

        migrating_slots = slots[: len(slots) / 2]
        task.add_step(
            'migrate', src_host=node.host, src_port=node.port,
            dst_host=new_host, dst_port=6379, slots=migrating_slots)
        logging.info('Migrating %d slots from %s to %s',
                     len(migrating_slots), host, new_host)
        db.session.add(task)
        db.session.flush()
        lock = task.acquire_lock()
        if lock is not None:
            logging.info('Auto balance task %d has been emit; lock id=%d',
                         task.id, lock.id)
            for h in hosts:
                models.audit.eru_event(
                    h, 6379, models.audit.EVENT_TYPE_CREATE,
                    app.default_user_id(), plan.balance_plan_json)
            return app.write_polling_targets()
        logging.info('Auto balance task fail to lock,'
                     ' discard auto balance this time.'
                     ' Delete container id=%s', cids)
        _rm_containers(cids, app)
    except BaseException as exc:
        logging.info('Remove container %s and rollback', cids)
        _rm_containers(cids, app)
        db.session.rollback()
        raise
