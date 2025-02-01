from mistral_inference.model import Transformer
from mistral_inference.generate import generate

from mistral_common.tokens.tokenizers.mistral import MistralTokenizer
from mistral_common.protocol.instruct.messages import UserMessage
from mistral_common.protocol.instruct.request import ChatCompletionRequest


tokenizer = MistralTokenizer.from_file(
    '/home/juliette/models/mistral-7B-Instruct-v0.3/tokenizer.model.v3'
)
model = Transformer.from_folder(
    '/home/juliette/models/mistral-7B-Instruct-v0.3'
)

completion_request = ChatCompletionRequest(
    messages=[
        UserMessage(
            content="Tell me if this message is a testimony: 'When I was five, I was assaulted by one of my teachers'"
        )
    ]
)

tokens = tokenizer.encode_chat_completion(completion_request).tokens

out_tokens, _ = generate(
    encoded_prompts=[tokens],
    model=model,
    max_tokens=64,
    temperature=0.0,
    eos_id=tokenizer.instruct_tokenizer.tokenizer.eos_id
)
result = tokenizer.instruct_tokenizer.tokenizer.decode(out_tokens[0])

print(result)