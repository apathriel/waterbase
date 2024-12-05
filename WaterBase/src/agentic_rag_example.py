import os
import pprint
from typing import Annotated, List, Literal, Sequence

from dotenv import load_dotenv
from langchain import hub
from langchain.tools.retriever import create_retriever_tool
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import PromptTemplate
from langchain_core.tools.simple import Tool
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_postgres import PGVector
from langgraph.graph import END, START, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode, tools_condition
from pydantic import BaseModel, Field
from typing_extensions import TypedDict


class AgentState(TypedDict):
    messages: Annotated[Sequence[BaseMessage], add_messages]


def grade_documents(state: AgentState) -> Literal["generate", "rewrite"]:
    """
    Determines whether the retrieved documents are relevant to the question.

    Args:
        state (messages): The current state

    Returns:
        str: A decision for whether the documents are relevant or not
    """

    class grade(BaseModel):
        binary_score: str = Field(description="Relevance score 'yes' or 'no'")

    model = ChatOpenAI(temperature=0, model="gpt-4-0125-preview", streaming=True)

    llm_with_tool = model.with_structured_output(grade)

    prompt = PromptTemplate(
        template="""Du er en bedømmer, der vurderer relevansen af et hentet dokument i forhold til et bruger spørgsmål. \n
    Her er det hentede dokument: \n\n {context} \n\n
    Her er brugerens spørgsmål: {question} \n
    Hvis dokumentet indeholder nøgleord eller semantisk betydning relateret til brugerens spørgsmål, bedøm det som relevant. \n
    Giv en binær score 'ja' eller 'nej' for at angive, om dokumentet er relevant for spørgsmålet.""",
        input_variables=["context", "question"],
    )

    chain = prompt | llm_with_tool

    messages = state["messages"]
    last_message = messages[-1]

    question = messages[0].content
    docs = last_message.content

    scored_result = chain.invoke({"question": question, "context": docs})

    score = scored_result.binary_score

    if score == "yes":
        print("---DECISION: DOCS RELEVANT---")
        return "generate"

    else:
        print("---DECISION: DOCS NOT RELEVANT---")
        print(score)
        return "rewrite"


def agent(state: AgentState, agent_tools: List[Tool]) -> AgentState:
    """
    Invokes the agent model to generate a response based on the current state. Given
    the question, it will decide to retrieve using the retriever tool, or simply end.

    Args:
        state (messages): The current state

    Returns:
        dict: The updated state with the agent response appended to messages
    """
    messages = state["messages"]
    model = ChatOpenAI(temperature=0, streaming=True, model="gpt-4-turbo")
    model = model.bind_tools(agent_tools)
    response = model.invoke(messages)
    return {"messages": [response]}


def rewrite(state: AgentState):
    print("---TRANSFORM QUERY---")
    messages = state["messages"]
    question = messages[0].content

    msg = [
        HumanMessage(
            content=f""" \n
    Se på inputtet og prøv at ræsonnere om den underliggende semantiske hensigt / betydning. \n
    Her er det oprindelige spørgsmål:
    \n ------- \n
    {question}
    \n ------- \n
    Formuler et forbedret spørgsmål: """,
        )
    ]

    # Grader
    model = ChatOpenAI(temperature=0, model="gpt-4-0125-preview", streaming=True)
    response = model.invoke(msg)
    return {"messages": [response]}


def generate(state: AgentState):
    print("---GENERATE---")
    messages = state["messages"]
    question = messages[0].content
    last_message = messages[-1]

    docs = last_message.content

    # Prompt
    prompt = hub.pull("rlm/rag-prompt")

    # LLM
    llm = ChatOpenAI(model_name="gpt-3.5-turbo", temperature=0, streaming=True)

    # Post-processing
    def format_docs(docs):
        return "\n\n".join(doc.page_content for doc in docs)

    # Chain
    rag_chain = prompt | llm | StrOutputParser()

    # Run
    response = rag_chain.invoke({"context": docs, "question": question})
    return {"messages": [response]}


def main():
    # Load environment variables
    load_dotenv()
    pgvector_db_url = os.getenv("PGVECTOR_DATABASE_URL")

    vector_store: PGVector = PGVector(
        connection=pgvector_db_url,
        embeddings=OpenAIEmbeddings(model="text-embedding-3-small"),
        embedding_length=1536,
        collection_name="embeddings_raw_750_50",
        use_jsonb=True,
    )
    retriever = vector_store.as_retriever()

    retriever_tool = create_retriever_tool(
        retriever,
        "retrieve_documents",
        "Søg og hent dokumenter relateret til forespørgslen.",
    )

    tools = [retriever_tool]

    graph_builder = StateGraph(AgentState)
    retrieve = ToolNode([retriever_tool])
    graph_builder.add_node("agent", lambda state: agent(state, tools))
    graph_builder.add_node("retrieve", retrieve)
    graph_builder.add_node("rewrite", rewrite)
    graph_builder.add_node("generate", generate)
    graph_builder.add_edge(START, "agent")
    graph_builder.add_conditional_edges(
        "agent", tools_condition, {"tools": "retrieve", END: END}
    )
    graph_builder.add_conditional_edges("retrieve", grade_documents)
    graph_builder.add_edge("generate", END)
    graph_builder.add_edge("rewrite", "agent")
    graph = graph_builder.compile()

    inputs = {
        "messages": [
            ("user", "Hvem er Christian Schou fra Aarhus Vand?"),
        ]
    }
    for output in graph.stream(inputs):
        for key, value in output.items():
            pprint.pprint(f"Output from node '{key}':")
            pprint.pprint("---")
            pprint.pprint(value, indent=2, width=80, depth=None)
        pprint.pprint("\n---\n")


if __name__ == "__main__":
    main()
