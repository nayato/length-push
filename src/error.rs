#![allow(unused_doc_comment)]

error_chain!{
    errors {
        FrameTooBig(n: Option<usize>) {
            description("Frame is too big")
            display("Frame is too big: '{:?}'", n)
        }
    }
    foreign_links{
        Io(::std::io::Error);
        Canceled(::futures::Canceled);
        Utf8Error(::std::str::Utf8Error);
    }
}