Feature: Get Dataranges

    Scenario: Get dataranges for an existing dataset
        Given a dataset with ID "test1" exists
        And the dataset contains 3 data points
        When I send a GET request to "/api/v1/datas3t/test1/dataranges"
        Then the response status should be 200
        And the response should contain 1 datarange
        And the datarange should have min_datapoint_key 1
        And the datarange should have max_datapoint_key 3
        And the datarange should have size_bytes greater than 0

    Scenario: Get dataranges for a non-existent dataset
        When I send a GET request to "/api/v1/datas3t/nonexistent/dataranges"
        Then the response status should be 404
        And the response body should be "dataset not found" 