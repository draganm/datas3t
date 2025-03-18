Feature: List Datasets

    Scenario: List datasets when none exist
        When I send a GET request to "/api/v1/datas3t"
        Then the response status should be 200
        And the response should contain 0 datasets

    Scenario: List datasets with datasets in the system
        Given a dataset with ID "test1" exists
        And the dataset contains 3 data points
        And a dataset with ID "test2" exists
        When I send a GET request to "/api/v1/datas3t"
        Then the response status should be 200
        And the response should contain 2 datasets
        And the response should contain a dataset with ID "test1"
        And the response should contain a dataset with ID "test2"
        And the dataset "test1" should have 1 datarange
        And the dataset "test1" should have size_bytes greater than 0 