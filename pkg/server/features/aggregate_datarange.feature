Feature: Aggregate datarange
  As a user of the datas3t service
  I want to aggregate multiple dataranges into a single one
  So that I can optimize storage and access performance

@wip
  Scenario: Aggregating multiple dataranges
    Given a dataset with ID "aggregate-test" exists
    And I upload a datapoint range containing 2 datapoints with keys 1 and 2
    And I upload a datapoint range containing 2 datapoints with keys 3 and 4
    And I upload a datapoint range containing 2 datapoints with keys 5 and 6
    When I send a POST request to "/api/v1/datas3t/aggregate-test/aggregate/1/6"
    Then the response status should be 200
    And the aggregated datarange should have start key 1
    And the aggregated datarange should have end key 6
    And the aggregated datarange should have replaced 3 ranges

  Scenario: Attempting to aggregate non-existent dataranges
    Given a dataset with ID "empty-dataset" exists
    When I send a POST request to "/api/v1/datas3t/empty-dataset/aggregate/1/10"
    Then the response status should be 404

  Scenario: Attempting to aggregate with invalid range
    Given a dataset with ID "invalid-range-test" exists
    And I upload a datapoint range containing 2 datapoints with keys 1 and 2
    When I send a POST request to "/api/v1/datas3t/invalid-range-test/aggregate/10/5"
    Then the response status should be 400 