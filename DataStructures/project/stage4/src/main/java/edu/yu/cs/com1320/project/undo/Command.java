package edu.yu.cs.com1320.project.undo;

import java.net.URI;
import java.util.function.Consumer;

public final class Command
{
    /**the URI of the document this command was executed on*/
    private URI url;
    private Consumer<URI> undo;
    public Command(URI url, Consumer<URI> undo)
    {
        this.url = url;
        this.undo = undo;
    }

    /**@return the URI of the document this command was executed on*/
    public URI getUri()
    {
        return this.url;
    }

    public void undo()
    {
        undo.accept(this.url);
    }
}