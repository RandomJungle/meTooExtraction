from typing import List

from googletrans import Translator


def translate_texts(texts: List, source: str = "ja", destination: str = "en"):
    translator = Translator()
    translations = translator.translate(
        texts,
        src=source,
        dest=destination
    )
    return translations


if __name__ == "__main__":

    texts_list = [
        '中学時代に制服をきていた女性は、私服通学していた人よりも、'
        '電車やバス、道路などで体を触られるなどの被害に多くあっていることが、'
        'ハラスメントを無くそうと有志で活動している'
        '「#WeTooJapan」の調査でわかりました。',
        '僕は、始発に近い電車にしました。\n7時だと女子高生と身体が密着する。'
        '\n\n痴漢に間違われるから、6時の始発\nに近い電車にしました。'
        '\n\n痴漢に間違われる。\nそれを、目当ての男性も多いと\n感じましたね。'
    ]
    translated_texts = translate_texts(texts_list, destination="fr")
    print(len(translated_texts))


