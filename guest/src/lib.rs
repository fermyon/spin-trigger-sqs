wit_bindgen_rust::export!("../sqs.wit");

struct Sqs;

impl sqs::Sqs for Sqs {
    fn handle_queue_message(message: sqs::Message,) -> Result<sqs::MessageAction, sqs::Error> {
        println!("I GOT A MESSAGE!  ID: {:?}", message.id);
        for attr in message.message_attributes {
            println!("  ... ATTR {}: {:?}", attr.name, attr.value);
        }
        println!("  ... BODY: {:?}", message.body);
        Ok(sqs::MessageAction::Delete)
    }
}
