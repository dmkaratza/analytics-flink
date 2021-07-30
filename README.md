# Small service that aggregates event data for articles.

The input event data a bounded stream, in a format of Visit events,
aggregating into hourly timeseries for each article.The service is implemented using Apache Flink,
taking advantage of the stateful operations and windowing functions.

### Example output

    | Document      | Start time       | End time         | Visits | Uniques | Time   | Completion |
    | ------------- | ---------------- | ---------------- | ------:| -------:| ------:| ----------:|
    | `7b2bc74e`... | 2015-04-22 13:00 | 2015-04-22 14:00 | 81,172 | 58,593  | 702.91 | 67,399     |
    | `7b2bc74e`... | 2015-04-22 12:00 | 2015-04-22 13:00 | 76,325 | 44,432  | 633.33 | 57,751     |
    | `7b2bc74e`... | 2015-04-22 11:00 | 2015-04-22 12:00 | 72,977 | 40,113  | 598.04 | 51,010     |
    | ⋮             | ⋮                | ⋮                | ⋮      | ⋮       | ⋮      | ⋮          |

    For example, between 12:00 and 13:00 on 22 April 2015, there were 76,325 visits to document
    `7b2bc74e-f529-4f5d-885b-4377c424211d` with a total of 633.33 hours of engaged time.
