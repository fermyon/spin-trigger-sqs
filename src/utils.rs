const UNKNOWN_ID: &str = "[unknown id]";

pub trait MessageUtils {
    fn display_id(&self) -> String;
}

impl MessageUtils for aws_sdk_sqs::model::Message {
    fn display_id(&self) -> String {
        self.message_id().unwrap_or(UNKNOWN_ID).to_owned()
    }
}

impl MessageUtils for crate::sqs::Message<'_> {
    fn display_id(&self) -> String {
        self.id.unwrap_or(UNKNOWN_ID).to_owned()
    }
}
