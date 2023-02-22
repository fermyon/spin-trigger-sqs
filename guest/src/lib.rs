wit_bindgen_rust::export!("../sqs.wit");

struct Sqs;

impl sqs::Sqs for Sqs {
    fn handle_queue_message(message: sqs::Message,) -> Result<sqs::MessageAction, sqs::Error> {
        println!("I GOT A MESSAGE!  ID: {:?}", message.id);
        for attr in message.message_attributes {
            println!("  ... ATTR {}: {:?}", attr.name, attr.value);
        }
        println!("  ... BODY: {:?}", message.body);

        if let Some(mid) = message.id {
            if let Some(last_char) = mid.chars().last() {
                if let Some(num) = last_char.to_digit(10) {
                    for i in 0..(num + 4) {
                        println!("  ... thinking for {i} secs");
                        std::thread::sleep(std::time::Duration::from_secs(i.into()));
                    }
                }
            }
        }

        Ok(sqs::MessageAction::Delete)
    }
}
