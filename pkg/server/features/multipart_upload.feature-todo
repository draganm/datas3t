Feature: Multipart upload to dataset
  As an API user
  I want to upload large files in multiple parts
  So that I can handle large datasets efficiently

  Background:
    Given an empty S3 bucket
    And an empty database
    And a dataset with ID "test-ds" is created
    
  Scenario: Initialize a multipart upload
    When I POST to "/api/v1/datas3t/test-ds/multipart"
    Then the response status should be 200
    And the response should contain a non-empty "upload_id"
    And the response should contain "dataset_id" with value "test-ds"
    
  Scenario: Upload a part to a multipart upload
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    When I POST part 1 to "/api/v1/datas3t/test-ds/multipart/upload-123/1"
    Then the response status should be 200
    And the response should contain "part_id" with value "1"

  Scenario: Upload multiple parts in sequence
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    And I have successfully uploaded part 1
    When I POST part 2 to "/api/v1/datas3t/test-ds/multipart/upload-123/2"
    Then the response status should be 200
    And the response should contain "part_id" with value "2"
    
  Scenario: Get status of a multipart upload
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    And I have uploaded parts 1 and 2
    When I GET "/api/v1/datas3t/test-ds/multipart/upload-123"
    Then the response status should be 200
    And the response should contain "upload_id" with value "upload-123"
    And the response should contain "dataset_id" with value "test-ds"
    And the response should contain "uploaded_parts" with values [1, 2]
    
  Scenario: List multipart uploads for a dataset
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    And I have initialized another multipart upload for dataset "test-ds" with ID "upload-456"
    When I GET "/api/v1/datas3t/test-ds/multipart"
    Then the response status should be 200
    And the response should contain "dataset_id" with value "test-ds"
    And the response should contain "uploads" as an array with 2 elements
    
  Scenario: Complete a multipart upload
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    And I have uploaded parts 1 and 2 with valid data
    When I POST to "/api/v1/datas3t/test-ds/multipart/upload-123/complete" with part IDs ["1", "2"]
    Then the response status should be 200
    And the response should contain "dataset_id" with value "test-ds"
    And the response should contain a non-empty "num_data_points"
    And the data should be stored in S3
    And the data range should be recorded in the database
    
  Scenario: Cancel a multipart upload
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    And I have uploaded part 1
    When I DELETE "/api/v1/datas3t/test-ds/multipart/upload-123"
    Then the response status should be 200
    And the response should contain "status" with value "cancelled"
    And the response should contain "upload_id" with value "upload-123"
    
  Scenario: Verify a cancelled upload is removed
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    And I have uploaded part 1
    And I have cancelled the upload
    When I GET "/api/v1/datas3t/test-ds/multipart/upload-123"
    Then the response status should be 404
    
  Scenario: Attempt to upload to a non-existent upload
    Given a dataset with ID "test-ds" is created
    When I POST part 1 to "/api/v1/datas3t/test-ds/multipart/non-existent-upload/1"
    Then the response status should be 404
    
  Scenario: Attempt to complete a non-existent upload
    Given a dataset with ID "test-ds" is created
    When I POST to "/api/v1/datas3t/test-ds/multipart/non-existent-upload/complete" with part IDs ["1"]
    Then the response status should be 404
    
  Scenario: Attempt to complete an upload with missing parts
    Given I have initialized a multipart upload for dataset "test-ds" with ID "upload-123"
    When I POST to "/api/v1/datas3t/test-ds/multipart/upload-123/complete" with part IDs ["999"]
    Then the response status should be 400
    
  Scenario: Attempt to initialize an upload for a non-existent dataset
    When I POST to "/api/v1/datas3t/non-existent-dataset/multipart"
    Then the response status should be 404 