# Copyright The Cloud Custodian Authors.
# SPDX-License-Identifier: Apache-2.0
import time
from c7n import utils
from c7n.exceptions import ResourceLimitExceeded
from c7n.policy import PullMode, execution
from c7n.version import version


@execution.register('in-memory-pull')
class InMemoryPullMode(PullMode):
    """Pull mode execution that skips writing output files to disk.
    """
    schema = utils.type_schema('in-memory-pull')

    def run(self, *args, **kw):
        if not self.policy.is_runnable():
            return []

        with self.policy.ctx as ctx:
            self.policy.log.debug(
                "Running policy:%s resource:%s region:%s c7n:%s",
                self.policy.name,
                self.policy.resource_type,
                self.policy.options.region or 'default',
                version,
            )

            s = time.time()
            try:
                resources = self.policy.resource_manager.resources()
            except ResourceLimitExceeded as e:
                self.policy.log.error(str(e))
                ctx.metrics.put_metric(
                    'ResourceLimitExceeded', e.selection_count, "Count"
                )
                raise

            rt = time.time() - s
            self.policy.log.info(
                "policy:%s resource:%s region:%s count:%d time:%0.2f",
                self.policy.name,
                self.policy.resource_type,
                self.policy.options.region,
                len(resources),
                rt,
            )
            ctx.metrics.put_metric(
                "ResourceCount", len(resources), "Count", Scope="Policy"
            )
            ctx.metrics.put_metric("ResourceTime", rt, "Seconds", Scope="Policy")

            if not resources:
                return []

            return resources