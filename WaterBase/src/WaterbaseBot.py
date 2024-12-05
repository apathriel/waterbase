# TO DO
# Implement memory
# Implement rewrite
# Implement multi query
# Implement document grading
# Add PDFs to the database

import os
import uuid
from typing import Annotated, List, Sequence, TypedDict

import streamlit as st
from dotenv import load_dotenv
from langchain.tools.retriever import create_retriever_tool
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.tools.simple import Tool
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_postgres import PGVector
from langgraph.checkpoint.memory import MemorySaver
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from psycopg import Connection


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]


def reasoner_agent(state: AgentState, agent_tools: List[Tool]) -> AgentState:
    """
    This agent is the start node. Responsible for deciding whether to call the retriever tool or not.
    """
    messages = state["messages"]
    model = ChatOpenAI(temperature=0, streaming=True, model="gpt-4o-mini")
    model = model.bind_tools(agent_tools)
    response = model.invoke(messages)
    return {"messages": [response]}


def generator_agent(state: AgentState):
    print("---GENERATE---")
    messages = state["messages"]
    question = messages[0].content
    last_message = messages[-1]

    docs = last_message.content

    template = """Du er en assistent for spørgsmål-svar opgaver.
        Brug følgende stykker af hentet kontekst til at besvare spørgsmålet.
        Hvis du ikke kender svaret, så sig blot, at du ikke ved det.
        Brug maksimalt tre sætninger og hold svaret kortfattet.

        Spørgsmål: {question}
        Kontekst: {context}
        Svar:"""

    prompt = ChatPromptTemplate.from_template(template)

    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0, streaming=True)

    rag_chain = prompt | llm | StrOutputParser()

    response = rag_chain.invoke({"question": question, "context": docs})
    return {"messages": [response]}


def main():
    # Load environment variables
    load_dotenv()
    pgvector_db_url = os.getenv("PGVECTOR_DATABASE_URL")
    base_db_url = os.getenv("DATABASE_URL")

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

    connection_kwargs = {
        "autocommit": True,
        "prepare_threshold": 0,
    }

    # with PostgresSaver.from_conn_string(base_db_url) as checkpointer:
    with Connection.connect(base_db_url, **connection_kwargs) as conn:
        checkpointer = PostgresSaver(conn)
        # checkpointer.setup()
        # Compile the graph
        graph = graph_builder.compile(checkpointer=checkpointer)
        config = {"configurable": {"thread_id": str(uuid.uuid4())}}

        first_msg = graph.invoke(
            {HumanMessage(content="Hvad er Water Living Lab?")}, config
        )

        second_msg = graph.invoke(
            {HumanMessage(content="Hvor er det baseret henne?")}, config
        )

        print(first_msg)
        print(second_msg)


if __name__ == "__main__":
    main()
