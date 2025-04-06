Feature: Delete dataset
  As a user
  I want to delete a dataset
  So that I can remove unnecessary data and free up resources

  Scenario: Successfully deleting an existing dataset
    Given a dataset with ID "test-delete-dataset" exists
    And I upload a datapoint range containing 5 data points to the dataset with ID "test-delete-dataset"
    When I send a DELETE request to "/api/v1/datas3t/test-delete-dataset"
    Then the response status should be 204
    And the dataset with the id "test-delete-dataset" should not exist
    And the dataset's dataranges should be deleted
    And the dataset's objects should be scheduled for deletion

  Scenario: Attempt to delete a non-existent dataset
    When I send a DELETE request to "/api/v1/datas3t/non-existent-dataset"
    Then the response status should be 404 