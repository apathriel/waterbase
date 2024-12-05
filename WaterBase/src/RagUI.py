import os
import random
import uuid

import streamlit as st
from dotenv import load_dotenv
from langchain.tools.retriever import create_retriever_tool
from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_postgres import PGVector
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.prebuilt import ToolNode
from psycopg import Connection
from WaterbaseBot import (
    END,
    START,
    AgentState,
    StateGraph,
    generator_agent,
    reasoner_agent,
    tools_condition,
)


def initialize_graph():
    """Initialize the LangGraph workflow."""
    # Load environment variables
    load_dotenv()
    pgvector_db_url = os.getenv("PGVECTOR_DATABASE_URL")

    # Setup vector store and retriever
    vector_store: PGVector = PGVector(
        connection=pgvector_db_url,
        embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
        collection_name="embeddings_raw_750_50",
        use_jsonb=True,
    )

    retriever = vector_store.as_retriever()

    retriever_tool = create_retriever_tool(
        retriever,
        "hent_dokumenter",
        "Søg og hent dokumenter relateret til forespørgslen.",
    )

    reasoner_agent_tools = [retriever_tool]
    retrieve_node = ToolNode([retriever_tool])

    # Build the graph
    graph_builder = StateGraph(AgentState)

    # Add nodes
    graph_builder.add_node(
        "reasoner", lambda state: reasoner_agent(state, reasoner_agent_tools)
    )
    graph_builder.add_node("retrieve", retrieve_node)
    graph_builder.add_node("generate", generator_agent)

    # Define edges
    graph_builder.add_edge(START, "reasoner")
    graph_builder.add_conditional_edges(
        "reasoner", tools_condition, {"tools": "retrieve", END: END}
    )
    graph_builder.add_edge("retrieve", "generate")
    graph_builder.add_edge("generate", END)

    return graph_builder


import uuid

import streamlit as st


def main():
    st.set_page_config(page_title="WaterbaseBot", page_icon="💬")
    st.title("🤖 Aarhus Vand RAG Chatbot")

    # Initialize thread_id and messages in session state
    if "thread_id" not in st.session_state:
        st.session_state.thread_id = str(uuid.uuid4())

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if "agent_state" not in st.session_state:
        st.session_state.agent_state = {"messages": []}

    # Initialize graph and checkpointer
    graph_builder = initialize_graph()
    base_db_url = os.getenv("DATABASE_URL")

    with Connection.connect(base_db_url, autocommit=True, prepare_threshold=0) as conn:
        checkpointer = PostgresSaver(conn)
        graph = graph_builder.compile(checkpointer=checkpointer)

        # Configuration using the consistent thread_id
        config = {"configurable": {"thread_id": st.session_state.thread_id}}

        # Display previous messages
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

        if prompt := st.chat_input("Ask me anything about Aarhusvand"):
            # Prepare initial state matching AgentState structure
            st.session_state.messages.append({"role": "user", "content": prompt})

            with st.chat_message("user"):
                st.markdown(prompt)

            with st.chat_message("assistant"):
                with st.spinner("Thinking..."):
                    try:
                        # Invoke graph with initial state
                        initial_state = st.session_state.agent_state
                        initial_state["messages"].append(HumanMessage(content=prompt))
                        response = graph.invoke(initial_state, config=config)

                        # Extract assistant's response
                        assistant_response = response["messages"][-1]

                        # Display and store assistant's response
                        st.markdown(assistant_response.content)
                        st.session_state.messages.append(
                            {"role": "assistant", "content": assistant_response.content}
                        )
                        st.session_state.agent_state["messages"].extend(
                            response["messages"]
                        )
                    except Exception as e:
                        st.error(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
