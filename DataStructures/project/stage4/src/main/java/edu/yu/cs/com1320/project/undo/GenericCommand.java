package edu.yu.cs.com1320.project.undo;

import java.util.function.Consumer;

/**
 * models a single action on any type of Target and an undo function to undo the command.
 * undo can only be called once on a given command.
 * @param <Target>
 */
public class GenericCommand<Target> implements Undoable
{
    /**the Target this command was executed on*/
    private Target target;
    private Consumer<Target> undoFunction;
    private boolean undone;
    public GenericCommand(Target target, Consumer<Target> undoFunction){
        this.target = target;
        this.undoFunction = undoFunction;
        this.undone = false;
    }

    /**@return the Target the command was executed on*/
    public Target getTarget(){
        return this.target;
    }

    /**
     * apply the undo function
     * @return true or false if the undo succeeds or fails
     * @throws IllegalStateException if the command has already been undone
     */
    @Override
    public boolean undo(){
        if (this.undone) {
            throw new IllegalStateException("this command has already been undone");
        }
        this.undoFunction.accept(this.target);
        this.undone = true;
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o){
            return true;
        }
        if (!(o instanceof GenericCommand)){
            return false;
        }
        GenericCommand<?> genericCommand = (GenericCommand<?>) o;
        return this.target.equals(genericCommand.target);
    }

    @Override
    public int hashCode() {
        return target.hashCode();
    }
}