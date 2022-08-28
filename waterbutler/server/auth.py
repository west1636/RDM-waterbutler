import inspect  # noqa

import logging
from stevedore import driver

from waterbutler.utils import inspect_info  # noqa
from waterbutler.core.auth import AuthType

logger = logging.getLogger(__name__)


class AuthHandler:

    def __init__(self, names):
        self.manager = driver.NamedExtensionManager(
            namespace='waterbutler.auth',
            names=names,
            invoke_on_load=True,
            invoke_args=(),
            name_order=True,
        )

    async def fetch(self, request, bundle):
        for extension in self.manager.extensions:
            credential = await extension.obj.fetch(request, bundle)
            if credential:
                return credential
        raise AuthHandler('no valid credential found')

    async def get(self, resource, provider, request, action=None, auth_type=AuthType.SOURCE,
                  path='', version=None, location_id=None):
        # logger.debug('----{}:{}::{} from {}:{}::{}'.format(*inspect_info(inspect.currentframe(), inspect.stack())))
        for extension in self.manager.extensions:
            credential = await extension.obj.get(
                resource, provider, request, action=action, auth_type=auth_type,
                path=path, version=version, location_id=location_id)
            if credential:
                return credential
        raise AuthHandler('no valid credential found')
