package com.devraccoon.processes;

import com.devraccoon.models.GameType;
import com.devraccoon.models.PlayerLookingForGameEvent;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class PlayerGameMatch extends KeyedProcessFunction<Long, PlayerLookingForGameEvent, Tuple2<Long, Set<UUID>>> {


   private transient MapState<Long, Set<UUID>> stateOutput;

   @Override
   public void open(Configuration parameters) {

       this.stateOutput = getRuntimeContext()
               .getMapState(new MapStateDescriptor<>(
                       "delayedOutputs",
                       BasicTypeInfo.LONG_TYPE_INFO,
                       TypeInformation.of(new TypeHint<Set<UUID>>() {})
               ));
   }

   /*@Override
   public void processElement(PlayerLookingForGameEvent playerLookingForGameEvent, Context context, Collector<Tuple2<Long, Set<UUID>>> collector) throws Exception {
       long timerValue = playerLookingForGameEvent.getEventTime().plusSeconds(5).toEpochMilli();
       Set<UUID> uuids = stateOutput.get(timerValue);
       if(uuids==null) {
           uuids = new HashSet<>();
       }
       uuids.add(playerLookingForGameEvent.getPlayerId());
       stateOutput.put(timerValue, uuids);

       context.timerService().registerEventTimeTimer(timerValue);
   }*/

    @Override
    public void processElement(PlayerLookingForGameEvent playerLookingForGameEvent, Context context, Collector<Tuple2<Long, Set<UUID>>> collector) throws Exception {

    }

    /*@Override
   public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<Long, Set<UUID>>> out) throws Exception {
       final Set<UUID> uuids = stateOutput.get(timestamp);
       if(!uuids.isEmpty() && uuids.size()>=ctx.getCurrentKey()) {
           Long nop = Long.valueOf(ctx.getCurrentKey());
           out.collect(Tuple2.of(nop, uuids));
       }
   }*/
}
