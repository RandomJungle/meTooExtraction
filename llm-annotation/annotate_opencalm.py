import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

model = AutoModelForCausalLM.from_pretrained(
    "cyberagent/open-calm-3b",
    device_map="auto",
    torch_dtype=torch.float16)
tokenizer = AutoTokenizer.from_pretrained(
    "cyberagent/open-calm-1b"
)

inputs = tokenizer(
    "このツイートに性暴力の個人的体験の証言が含まれているのでしょうか。「先日もツイートしました。俺自身が小学生の時にアイスにつられ、男の家にノコノコついて行き、イタズラされた。誰にも言えなかった。数か月後、近所で性犯罪者が捕まった。俺が親告してれば被害者増やさずにすんだ。子供ながらに罪悪感に襲われた。だから詩織さんの行動に頭が下がる。　#MeToo 」",
    return_tensors="pt"
).to(model.device)
with torch.no_grad():
    tokens = model.generate(
        **inputs,
        max_new_tokens=64,
        do_sample=True,
        temperature=0.7,
        top_p=0.9,
        repetition_penalty=1.05,
        pad_token_id=tokenizer.pad_token_id,
    )

output = tokenizer.decode(tokens[0], skip_special_tokens=True)
print(output)
