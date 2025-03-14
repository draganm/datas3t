Feature: Upload to dataset

    Scenario: Upload to an empty dataset
        When I create a new dataset with ID "my-dataset"
        And I upload a dataset range containing 3 data points to the dataset with ID "my-dataset"
        Then the dataset should have 3 data points        
        And the s3 bucket should contain the dataset range
