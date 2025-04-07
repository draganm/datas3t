Feature: Get missing ranges for a dataset
  As a user
  I want to identify missing data point ranges
  So that I can fill in gaps in my dataset

  Scenario: Get missing ranges for a dataset with no dataranges
    Given a dataset with ID "test-no-ranges" exists
    When I send a GET request to "/api/v1/datas3t/test-no-ranges/missing-ranges"
    Then the response status should be 200
    And the response should contain empty missing ranges

  Scenario: Get missing ranges for a dataset with one contiguous datarange
    Given a dataset with ID "test-one-range" exists
    And I upload a datapoint range containing 2 datapoints with keys 1 and 2
    When I send a GET request to "/api/v1/datas3t/test-one-range/missing-ranges"
    Then the response status should be 200
    And the response should have first datapoint 1
    And the response should have last datapoint 2
    And the response should contain 0 missing ranges

  Scenario: Get missing ranges for a dataset with two non-contiguous dataranges
    Given a dataset with ID "test-two-ranges" exists
    And I upload a datapoint range containing 2 datapoints with keys 1 and 2
    And I upload a datapoint range containing 2 datapoints with keys 4 and 5
    When I send a GET request to "/api/v1/datas3t/test-two-ranges/missing-ranges"
    Then the response status should be 200
    And the response should have first datapoint 1
    And the response should have last datapoint 5
    And the response should contain 1 missing ranges
    And missing range 0 should have start 3 and end 3

  Scenario: Get missing ranges for a dataset with three non-contiguous dataranges
    Given a dataset with ID "test-three-ranges" exists
    And I upload a datapoint range containing 2 datapoints with keys 1 and 2
    And I upload a datapoint range containing 2 datapoints with keys 3 and 4
    And I upload a datapoint range containing 2 datapoints with keys 7 and 8
    When I send a GET request to "/api/v1/datas3t/test-three-ranges/missing-ranges"
    Then the response status should be 200
    And the response should have first datapoint 1
    And the response should have last datapoint 8
    And the response should contain 1 missing ranges
    And missing range 0 should have start 5 and end 6 