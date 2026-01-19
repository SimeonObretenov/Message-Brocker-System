package com.msgbroker.util.mock;

import java.util.Queue;

import static com.msgbroker.util.CommandBuilder.elect;
import static com.msgbroker.util.CommandBuilder.vote;
import static com.msgbroker.util.CommandBuilder.declare;
import static com.msgbroker.util.CommandBuilder.ack;
import static com.msgbroker.util.CommandBuilder.PING;
import static com.msgbroker.util.CommandBuilder.PONG;
import static com.msgbroker.util.CommandBuilder.OK;

public class CommandExpectationFactory {

    private final int electionId;
    private final Queue<CommandResponse> expectations;

    public CommandExpectationFactory(int electionId, Queue<CommandResponse> expectations) {
        this.electionId = electionId;
        this.expectations = expectations;
    }

    private CommandExpectationFactory expectCommandReturnResponse(String expectedCommand, String response) {
        this.expectations.add(new CommandResponse(expectedCommand, response));
        return this;
    }

    public CommandExpectationFactory expectElectReturnOk(int id) {
        return expectCommandReturnResponse(elect(id), OK);
    }

    public CommandExpectationFactory expectElectReturnVote(int electId, int candidateId) {
        return expectCommandReturnResponse(elect(electId), vote(electionId, candidateId));
    }

    public CommandExpectationFactory expectDeclareReturnAck(int id) {
        return expectCommandReturnResponse(declare(id), ack(electionId));
    }

    public CommandExpectationFactory expectPingReturnPong() {
        return expectCommandReturnResponse(PING, PONG);
    }
}
