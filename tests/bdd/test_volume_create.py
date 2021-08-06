"""Volume creation feature tests."""

from pytest_bdd import (
    given,
    scenario,
    then,
    when,
)


@scenario("volume/create.feature", "provisioning failure")
def test_provisioning_failure():
    """provisioning failure."""


@scenario("volume/create.feature", "spec cannot be satisfied")
def test_spec_cannot_be_satisfied():
    """spec cannot be satisfied."""


@scenario("volume/create.feature", "successful creation")
def test_successful_creation():
    """successful creation."""


@given("a control plane")
def a_control_plane():
    """a control plane."""
    raise NotImplementedError


@given("one or more Mayastor instances")
def one_or_more_mayastor_instances():
    """one or more Mayastor instances."""
    raise NotImplementedError


@when("a user attempts to create a volume")
def a_user_attempts_to_create_a_volume():
    """a user attempts to create a volume."""
    raise NotImplementedError


@when("during the provisioning there is a failure")
def during_the_provisioning_there_is_a_failure():
    """during the provisioning there is a failure."""
    raise NotImplementedError


@when("the request can initially be satisfied")
def the_request_can_initially_be_satisfied():
    """the request can initially be satisfied."""
    raise NotImplementedError


@when("the spec of the volume cannot be satisfied")
def the_spec_of_the_volume_cannot_be_satisfied():
    """the spec of the volume cannot be satisfied."""
    raise NotImplementedError


@when("there are at least as many suitable pools as there are requested replicas")
def there_are_at_least_as_many_suitable_pools_as_there_are_requested_replicas():
    """there are at least as many suitable pools as there are requested replicas."""
    raise NotImplementedError


@then("the partially created volume should eventually be cleaned up")
def the_partially_created_volume_should_eventually_be_cleaned_up():
    """the partially created volume should eventually be cleaned up."""
    raise NotImplementedError


@then("the reason the volume could not be created should be returned")
def the_reason_the_volume_could_not_be_created_should_be_returned():
    """the reason the volume could not be created should be returned."""
    raise NotImplementedError


@then("the volume object should be returned")
def the_volume_object_should_be_returned():
    """the volume object should be returned."""
    raise NotImplementedError


@then("the volume should be created")
def the_volume_should_be_created():
    """the volume should be created."""
    raise NotImplementedError


@then("the volume should not be created")
def the_volume_should_not_be_created():
    """the volume should not be created."""
    raise NotImplementedError
