import streamlit as st
import pandas as pd
import sqlite3
import plotly.express as px
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

# ---------------------
# モデル読み込み（初回だけ）
# ---------------------
@st.cache_resource
def load_model():
    tokenizer = AutoTokenizer.from_pretrained("cyberagent/calm3-22b-chat")
    model = AutoModelForCausalLM.from_pretrained(
        "cyberagent/calm3-22b-chat",
        device_map="auto",
        torch_dtype=torch.float16  # もしくは "auto"
    )
    return model, tokenizer

st.title("💬 Text2SQL チャット × CALM3")
st.markdown("自然言語で質問すると、SQLを生成してグラフ化します。")

model, tokenizer = load_model()

# ---------------------
# データ読み込み
# ---------------------
uploaded_file = st.file_uploader("📄 CSVまたはParquetファイルをアップロード", type=["csv", "parquet"])

if uploaded_file:
    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_parquet(uploaded_file)
    st.success("✅ データ読み込み成功！")
    st.dataframe(df.head())

    conn = sqlite3.connect(":memory:")
    df.to_sql("data", conn, index=False, if_exists="replace")

    # ---------------------
    # チャット入力
    # ---------------------
    user_input = st.chat_input("自然言語でデータに質問してみよう（例：月別の売上合計）")

    if user_input:
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            with st.spinner("SQLを生成中..."):

                # テーブル情報
                table_info = "\n".join([f"{col}: {str(dtype)}" for col, dtype in zip(df.columns, df.dtypes)])

                # プロンプト生成
                prompt = f"""あなたはデータアナリストです。
以下のテーブル構造に対して、質問に答えるSQLiteクエリを出力してください。

テーブル情報:
{table_info}

質問:
{user_input}

SQL:"""

                # 入力トークン作成
                messages = [
                    {"role": "system", "content": "あなたはSQLクエリを生成するアシスタントです。"},
                    {"role": "user", "content": prompt}
                ]
                input_ids = tokenizer.apply_chat_template(messages, add_generation_prompt=True, return_tensors="pt").to(model.device)

                # モデル推論
                output_ids = model.generate(input_ids, max_new_tokens=256, temperature=0.3)
                response = tokenizer.decode(output_ids[0], skip_special_tokens=True)

                # SQL抽出
                sql_start = response.find("SELECT")
                sql_query = response[sql_start:] if sql_start >= 0 else response

                st.markdown(f"🧠 **生成されたSQLクエリ**:\n```sql\n{sql_query}\n```")

                # SQL実行
                try:
                    result_df = pd.read_sql_query(sql_query, conn)
                    st.dataframe(result_df)

                    # グラフ表示（2列）
                    if result_df.shape[1] == 2:
                        fig = px.bar(result_df, x=result_df.columns[0], y=result_df.columns[1])
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("2列の結果のみグラフ化されます。")
                except Exception as e:
                    st.error(f"❌ SQL実行エラー: {e}")
else:
    st.info("まずはCSVまたはParquetをアップロードしてください。")
