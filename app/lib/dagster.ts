const DAGSTER_GRAPHQL_URL =
  process.env.DAGSTER_GRAPHQL_URL ?? "http://dagster-webserver:3000/graphql";

type GraphQLResponse<T> = {
  data?: T;
  errors?: Array<{ message: string }>;
};

async function queryDagster<T>(query: string, variables: object): Promise<T> {
  const response = await fetch(DAGSTER_GRAPHQL_URL, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ query, variables }),
    cache: "no-store",
    signal: AbortSignal.timeout(15_000),
  });

  if (!response.ok) {
    throw new Error(`Dagster respondió HTTP ${response.status}.`);
  }

  const result = (await response.json()) as GraphQLResponse<T>;
  if (result.errors?.length) {
    throw new Error(result.errors.map((error) => error.message).join(" "));
  }
  if (!result.data) {
    throw new Error("Dagster no devolvió datos.");
  }
  return result.data;
}

const LAUNCH_INGESTION = `
  mutation LaunchIngestion($executionParams: ExecutionParams!) {
    launchPipelineExecution(executionParams: $executionParams) {
      __typename
      ... on LaunchRunSuccess { run { runId status } }
      ... on LaunchPipelineRunSuccess { run { runId status } }
      ... on RunConfigValidationInvalid {
        errors { message path reason }
      }
      ... on PipelineNotFoundError { message }
      ... on PythonError { message }
      ... on UnauthorizedError { message }
    }
  }
`;

type LaunchPayload = {
  launchPipelineExecution: {
    __typename: string;
    run?: { runId: string; status: string };
    message?: string;
    errors?: Array<{ message: string }>;
  };
};

export async function launchIngestion(dataset: string, filePath: string) {
  const data = await queryDagster<LaunchPayload>(LAUNCH_INGESTION, {
    executionParams: {
      selector: {
        repositoryLocationName: "lakehouse_pipeline",
        repositoryName: "__repository__",
        pipelineName: "ingest_export_job",
      },
      runConfigData: {
        ops: {
          ingest_export: {
            config: { dataset, file_path: filePath },
          },
        },
      },
      mode: "default",
      executionMetadata: {
        tags: [
          { key: "source", value: "nextjs-upload" },
          { key: "dataset", value: dataset },
        ],
      },
    },
  });

  const result = data.launchPipelineExecution;
  if (!result.run) {
    const details = result.errors?.map((error) => error.message).join(" ");
    throw new Error(details || result.message || `Dagster rechazó la ejecución: ${result.__typename}.`);
  }
  return result.run;
}

const GET_RUN = `
  query GetRun($runId: ID!) {
    pipelineRunOrError(runId: $runId) {
      __typename
      ... on Run { runId status startTime endTime }
      ... on RunNotFoundError { message }
      ... on PythonError { message }
    }
  }
`;

type RunPayload = {
  pipelineRunOrError: {
    __typename: string;
    runId?: string;
    status?: string;
    startTime?: number | null;
    endTime?: number | null;
    message?: string;
  };
};

export async function getRun(runId: string) {
  const data = await queryDagster<RunPayload>(GET_RUN, { runId });
  const result = data.pipelineRunOrError;
  if (!result.runId || !result.status) {
    throw new Error(result.message || "No se encontró la ejecución en Dagster.");
  }
  return result;
}
