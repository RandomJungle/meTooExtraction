import torch
from transformers import AutoTokenizer, AutoModelForCausalLM

tokenizer = AutoTokenizer.from_pretrained("rinna/bilingual-gpt-neox-4b-instruction-ppo", use_fast=False)
model = AutoModelForCausalLM.from_pretrained("rinna/bilingual-gpt-neox-4b-instruction-ppo")

if torch.cuda.is_available():
    model = model.to("cuda")

prompt = """
このツイートに性暴力の個人的体験の証言が含まれているのでしょうか。「先日もツイートしました。俺自身が小学生の時にアイスにつられ、男の家にノコノコついて行き、イタズラされた。誰にも言えなかった。数か月後、近所で性犯罪者が捕まった。俺が親告してれば被害者増やさずにすんだ。子供ながらに罪悪感に襲われた。だから詩織さんの行動に頭が下がる。　#MeToo 」
"""

token_ids = tokenizer.encode(prompt, add_special_tokens=False, return_tensors="pt")

with torch.no_grad():
    output_ids = model.generate(
        token_ids.to(model.device),
        max_new_tokens=512,
        do_sample=True,
        temperature=1.0,
        top_p=0.85,
        pad_token_id=tokenizer.pad_token_id,
        bos_token_id=tokenizer.bos_token_id,
        eos_token_id=tokenizer.eos_token_id
    )

output = tokenizer.decode(output_ids.tolist()[0][token_ids.size(1):])
print(output)