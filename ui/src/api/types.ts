export interface Datas3tInfo {
  datas3t_name: string;
  bucket_name: string;
  datarange_count: number;
  total_datapoints: number;
  lowest_datapoint: number;
  highest_datapoint: number;
  total_bytes: number;
}

export interface DatarangeInfo {
  datarange_id: number;
  data_object_key: string;
  index_object_key: string;
  min_datapoint_key: number;
  max_datapoint_key: number;
  size_bytes: number;
}

export interface ListDatarangesResponse {
  dataranges: DatarangeInfo[];
}

export interface BucketInfo {
  name: string;
  endpoint: string;
  bucket: string;
}
