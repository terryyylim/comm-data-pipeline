## Beam Pipeline (Ecommerce Data)
Setup:
1. Run terraform apply from `/infra` folder.
2. Manually create tmp folder inside bucket generated from TF for Dataflow jobs to run(i.e tempLocation).
3. Manually create table (eg. event_stream) inside dataset (eg. ecomm_demo).
    - Declare Schema in table under `Edit schema`.
    ```
    "event_type" => "STRING"
    "event_version" => "STRING"
    "created_at" => "STRING"
    "user_id" => "STRING"
    "device_type" => "STRING"
    "item_name" => "STRING"
    ```
4. Manually create a subscription for Pubsub Topic.
5. Run `./gradlew run` inside `/event-pipeline` folder.
6. Run `./gradlew run` inside `/event-pubsub` folder.

