Feature: Wait for datasets to reach certain datapoints
  In order to synchronize processing with data availability
  As an API consumer
  I want to be able to wait for datasets to reach specific datapoints

  Scenario: Wait for a single dataset to reach a specific datapoint
    Given a dataset with ID "test-wait-1" exists
    And I upload a datapoint range containing 5 data points to the dataset with ID "test-wait-1"
    When I send a POST request to "/api/v1/datas3t/wait" with datasets "test-wait-1" and datapoint 5
    Then the response status should be 200
    And the response should contain the dataset "test-wait-1" with datapoint 5

  Scenario: Wait for a non-existing dataset
    When I send a POST request to "/api/v1/datas3t/wait" with datasets "nonexistent-dataset" and datapoint 10
    Then the response status should be 400
    And the response should indicate the dataset "nonexistent-dataset" is missing

  Scenario: Wait for multiple datasets to reach specific datapoints
    Given a dataset with ID "test-wait-2" exists
    And a dataset with ID "test-wait-3" exists
    And I upload a datapoint range containing 10 data points to the dataset with ID "test-wait-2"
    And I upload a datapoint range containing 15 data points to the dataset with ID "test-wait-3"
    When I send a POST request to "/api/v1/datas3t/wait" with multiple datasets
      | dataset     | datapoint |
      | test-wait-2 | 8         |
      | test-wait-3 | 12        |
    Then the response status should be 200
    And the response should contain the dataset "test-wait-2" with datapoint 10
    And the response should contain the dataset "test-wait-3" with datapoint 15

  Scenario: Wait for a dataset with datapoint that hasn't been reached yet
    Given a dataset with ID "test-wait-4" exists
    And I upload a datapoint range containing 3 data points to the dataset with ID "test-wait-4"
    When I send a POST request to "/api/v1/datas3t/wait" with datasets "test-wait-4" and datapoint 10 and timeout 1
    Then the response status should be 202
    And the response should contain the dataset "test-wait-4" with datapoint 3