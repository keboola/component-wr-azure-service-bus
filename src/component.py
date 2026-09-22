"""Azure Service Bus writer — main component class."""

import logging
import sys

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from configuration import Configuration

logger = logging.getLogger(__name__)


class Component(ComponentBase):
    """Reads a Storage input table per config row and sends each row to Azure Service Bus."""

    def __init__(self):
        super().__init__()

    def run(self):
        """Main execution code."""
        Configuration(**self.configuration.parameters)


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException:
        logger.exception("Component failed with a user error")
        sys.exit(1)
    except Exception:
        logger.exception("Component failed with an unexpected error")
        sys.exit(2)
