Feature: Get Datarange

  Scenario: Get Datarange
    Given a dataset with ID "test-dataset" exists
    And the dataset contains 3 data points
    When I send a GET request to "/api/v1/datas3t/test-dataset/datarange/0/2"
    Then the response status should be 200
    And the response should return a list of one object and range
