package edu.yu.introtoalgs;

public class CountTheMoves extends CountTheMovesBase {
    private final BoardUnit[][] board;

    public CountTheMoves(BoardUnit[][] board) {
        super(board);
        this.board = board;
    }

    @Override
    public int countThem() {
        int total = 0;
        for (int i = 0; i < N_ROWS; i++) {
            for (int j = 0; j < N_COLUMNS; j++) {
                BoardUnit unit = board[i][j];
                if (unit == BoardUnit.HOLE) {
                    total += checkForLegalMoves(i, j);
                }
            }
        }
        return total;
    }

    private int checkForLegalMoves(int row, int col) {
        int count = 0;
        if (row >= 2 && board[row-1][col] == BoardUnit.PEG && board[row-2][col] == BoardUnit.PEG) {
            count++;
        }
        if (row <= N_ROWS - 3 && board[row+1][col] == BoardUnit.PEG && board[row+2][col] == BoardUnit.PEG) {
            count++;
        }
        if (col >= 2 && board[row][col-1] == BoardUnit.PEG && board[row][col-2] == BoardUnit.PEG) {
            count++;
        }
        if (col <= N_ROWS - 3 && board[row][col+1] == BoardUnit.PEG && board[row][col+2] == BoardUnit.PEG) {
            count++;
        }
        return count;
    }
}
