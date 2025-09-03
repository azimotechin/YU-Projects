package edu.yu.introtoalgs;

import org.junit.jupiter.api.RepeatedTest;
import static org.junit.jupiter.api.Assertions.*;
import edu.yu.introtoalgs.CountTheMovesBase.*;
import java.util.Random;


public class CountTheMovesTest {
    final int TEST_TIME = new Random().nextInt(100) + 1;

    public static class Stopwatch {
        private final long start;

        public Stopwatch() {
            start = System.nanoTime();
        }

        public long elapsedNanoseconds() {
            return System.nanoTime() - start;
        }
    }

    private BoardUnit[][] generateWorstCaseBoard() {
        BoardUnit[][] worstCase = new BoardUnit[CountTheMovesBase.N_ROWS][CountTheMovesBase.N_COLUMNS];
        for (int i = 0; i < CountTheMovesBase.N_ROWS; i++) {
            for (int j = 0; j < CountTheMovesBase.N_COLUMNS; j++) {
                if ((i < 2 || i > 4) && (j < 2 || j > 4)) {
                    worstCase[i][j] = BoardUnit.BOARD;
                } else {
                    worstCase[i][j] = BoardUnit.PEG;
                }
            }
        }
        worstCase[3][3] = BoardUnit.HOLE;
        worstCase[2][3] = BoardUnit.HOLE;
        worstCase[4][3] = BoardUnit.HOLE;
        worstCase[3][2] = BoardUnit.HOLE;
        worstCase[3][4] = BoardUnit.HOLE;
        return worstCase;
    }

    private BoardUnit[][] generateAllHolesBoard() {
        BoardUnit[][] holesBoard = new BoardUnit[CountTheMovesBase.N_ROWS][CountTheMovesBase.N_COLUMNS];
        for (int i = 0; i < CountTheMovesBase.N_ROWS; i++) {
            for (int j = 0; j < CountTheMovesBase.N_COLUMNS; j++) {
                if ((i < 2 || i > 4) && (j < 2 || j > 4)) {
                    holesBoard[i][j] = BoardUnit.BOARD;
                } else {
                    holesBoard[i][j] = BoardUnit.HOLE;
                }
            }
        }
        return holesBoard;
    }

    @RepeatedTest(5)
    public void testCountThemPerformanceWorstCase() {
        int iterations = TEST_TIME;
        long totalNanos = 0;
        for (int i = 0; i < 10; i++) {
            new CountTheMoves(generateWorstCaseBoard()).countThem();
        }
        for (int k = 0; k < iterations; k++) {
            CountTheMoves game = new CountTheMoves(generateWorstCaseBoard());
            Stopwatch timer = new Stopwatch();
            game.countThem();
            totalNanos += timer.elapsedNanoseconds();
        }
        long avgNanos = totalNanos / iterations;
        System.out.println("Average over " + iterations + ": " + avgNanos + " ns");
        assertTrue(avgNanos < 8_000);
    }

    @RepeatedTest(5)
    public void testCountThemPerformanceAllHoles() {
        int iterations = TEST_TIME;
        long totalNanos = 0;
        for (int i = 0; i < 10; i++) {
            new CountTheMoves(generateWorstCaseBoard()).countThem();
        }
        for (int k = 0; k < iterations; k++) {
            CountTheMoves game = new CountTheMoves(generateAllHolesBoard());
            Stopwatch timer = new Stopwatch();
            game.countThem();
            totalNanos += timer.elapsedNanoseconds();
        }
        long avgNanos = totalNanos / iterations;
        System.out.println("Average over " + iterations + ": " + avgNanos + " ns");
        assertTrue(avgNanos < 8_000);
    }
}
