namespace JobSvc.Models;

public enum JobStatus
{
    Queued,
    Printing,
    Requeued,
    Done,
    Error,
    Archived
}
