Feature: Create Dataset

    Scenario: Create a dataset
        When I send a PUT request to "/api/v1/datas3t/test1"
        Then the response status should be 204
        And the dataset with the id "test1" should exist