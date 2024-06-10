def direct_send(wallet_service: WalletService,
                mixdepth: int,
                dest_and_amounts: List[Tuple[str, int]],
                answeryes: bool = False,
                accept_callback: Optional[Callable[[str, str, int, int, Optional[str]], bool]] = None,
                info_callback: Optional[Callable[[str], None]] = None,
                error_callback: Optional[Callable[[str], None]] = None,
                return_transaction: bool = False,
                with_final_psbt: bool = False,
                optin_rbf: bool = True,
                custom_change_addr: Optional[str] = None,
                change_label: Optional[str] = None) -> Union[bool, str]:
    """Send coins directly from one mixdepth to one destination address;
    does not need IRC. Sweep as for normal sendpayment (set amount=0).
    If answeryes is True, callback/command line query is not performed.
    If optin_rbf is True, the nSequence values are changed as appropriate.
    If accept_callback is None, command line input for acceptance is assumed,
    else this callback is called:
    accept_callback:
    ====
    args:
    deserialized tx, destination address, amount in satoshis,
    fee in satoshis, custom change address

    returns:
    True if accepted, False if not
    ====
    info_callback and error_callback takes one parameter, the information
    message (when tx is pushed or error occured), and returns nothing.

    This function returns:
    1. False if there is any failure.
    2. The txid if transaction is pushed, and return_transaction is False,
       and with_final_psbt is False.
    3. The full CMutableTransaction if return_transaction is True and
       with_final_psbt is False.
    4. The PSBT object if with_final_psbt is True, and in
       this case the transaction is *NOT* broadcast.
    """
    is_sweep = False
    outtypes = []
    total_outputs_val = 0

    #Sanity checks
    assert isinstance(dest_and_amounts, list)
    assert len(dest_and_amounts) > 0
    assert custom_change_addr is None or validate_address(custom_change_addr)[0]
    assert isinstance(mixdepth, numbers.Integral)
    assert mixdepth >= 0
    assert isinstance(wallet_service.wallet, BaseWallet)

    for target in dest_and_amounts:
        destination = target[0]
        amount = target[1]
        assert validate_address(destination)[0] or \
            is_burn_destination(destination)
        if amount == 0:
            assert custom_change_addr is None and \
                len(dest_and_amounts) == 1
            is_sweep = True
        assert isinstance(amount, numbers.Integral)
        assert amount >= 0
        if is_burn_destination(destination):
            #Additional checks
            if not isinstance(wallet_service.wallet, FidelityBondMixin):
                log.error("Only fidelity bond wallets can burn coins")
                return
            if answeryes:
                log.error("Burning coins not allowed without asking for confirmation")
                return
            if mixdepth != FidelityBondMixin.FIDELITY_BOND_MIXDEPTH:
                log.error("Burning coins only allowed from mixdepth " + str(
                    FidelityBondMixin.FIDELITY_BOND_MIXDEPTH))
                return
            if amount != 0:
                log.error("Only sweeping allowed when burning coins, to keep "
                    "the tx small. Tip: use the coin control feature to "
                    "freeze utxos")
                return
        # if the output is of a script type not currently
        # handled by our wallet code, we can't use information
        # to help us calculate fees, but fall back to default.
        # This is represented by a return value `None`.
        # Note that this does *not* imply we accept any nonstandard
        # output script, because we already called `validate_address`.
        outtypes.append(wallet_service.get_outtype(destination))
        total_outputs_val += amount

    txtype = wallet_service.get_txtype()

    if is_sweep:
        #doing a sweep
        destination = dest_and_amounts[0][0]
        amount = dest_and_amounts[0][1]
        utxos = wallet_service.get_utxos_by_mixdepth()[mixdepth]
        if utxos == {}:
            log.error(
                f"There are no available utxos in mixdepth {mixdepth}, "
                 "quitting.")
            return
        total_inputs_val = sum([va['value'] for u, va in utxos.items()])
        script_types = get_utxo_scripts(wallet_service.wallet, utxos)
        fee_est = estimate_tx_fee(len(utxos), 1, txtype=script_types,
            outtype=outtypes[0])
        outs = [{"address": destination,
                 "value": total_inputs_val - fee_est}]
    else:
        if custom_change_addr:
            change_type = wallet_service.get_outtype(custom_change_addr)
            if change_type is None:
                # we don't recognize this type; best we can do is revert to
                # default, even though it may be inaccurate:
                change_type = txtype
        else:
            change_type = txtype
        if outtypes[0] is None:
            # we don't recognize the destination script type,
            # so set it as the same as the change (which will usually
            # be the same as the spending wallet, but see above for custom)
            # Notice that this is handled differently to the sweep case above,
            # because we must use a list - there is more than one output
            outtypes[0] = change_type
        outtypes.append(change_type)
        # not doing a sweep; we will have change.
        # 8 inputs to be conservative; note we cannot account for the possibility
        # of non-standard input types at this point.
        initial_fee_est = estimate_tx_fee(8, len(dest_and_amounts) + 1,
                                          txtype=txtype, outtype=outtypes)
        utxos = wallet_service.select_utxos(mixdepth, amount + initial_fee_est,
                                            includeaddr=True)
        script_types = get_utxo_scripts(wallet_service.wallet, utxos)
        if len(utxos) < 8:
            fee_est = estimate_tx_fee(len(utxos), len(dest_and_amounts) + 1,
                                      txtype=script_types, outtype=outtypes)
        else:
            fee_est = initial_fee_est
        total_inputs_val = sum([va['value'] for u, va in utxos.items()])
        changeval = total_inputs_val - fee_est - total_outputs_val
        outs = []
        for out in dest_and_amounts:
            outs.append({"value": out[1], "address": out[0]})
        change_addr = wallet_service.get_internal_addr(mixdepth) \
            if custom_change_addr is None else custom_change_addr
        outs.append({"value": changeval, "address": change_addr})

    #compute transaction locktime, has special case for spending timelocked coins
    tx_locktime = compute_tx_locktime()
    if mixdepth == FidelityBondMixin.FIDELITY_BOND_MIXDEPTH and \
            isinstance(wallet_service.wallet, FidelityBondMixin):
        for outpoint, utxo in utxos.items():
            path = wallet_service.script_to_path(utxo["script"])
            if not FidelityBondMixin.is_timelocked_path(path):
                continue
            path_locktime = path[-1]
            tx_locktime = max(tx_locktime, path_locktime+1)
            #compute_tx_locktime() gives a locktime in terms of block height
            #timelocked addresses use unix time instead
            #OP_CHECKLOCKTIMEVERIFY can only compare like with like, so we
            #must use unix time as the transaction locktime

    #Now ready to construct transaction
    log.info("Using a fee of: " + amount_to_str(fee_est) + ".")
    if not is_sweep:
        log.info("Using a change value of: " + amount_to_str(changeval) + ".")
    tx = make_shuffled_tx(list(utxos.keys()), outs,
                          version=2, locktime=tx_locktime)

    if optin_rbf:
        for inp in tx.vin:
            inp.nSequence = 0xffffffff - 2

    inscripts = {}
    spent_outs = []
    for i, txinp in enumerate(tx.vin):
        u = (txinp.prevout.hash[::-1], txinp.prevout.n)
        inscripts[i] = (utxos[u]["script"], utxos[u]["value"])
        spent_outs.append(CMutableTxOut(utxos[u]["value"],
                                        utxos[u]["script"]))
    if with_final_psbt:
        # here we have the PSBTWalletMixin do the signing stage
        # for us:
        new_psbt = wallet_service.create_psbt_from_tx(tx, spent_outs=spent_outs)
        serialized_psbt, err = wallet_service.sign_psbt(new_psbt.serialize())
        if err:
            log.error("Failed to sign PSBT, quitting. Error message: " + err)
            return False
        new_psbt_signed = PartiallySignedTransaction.deserialize(serialized_psbt)
        print("Completed PSBT created: ")
        print(wallet_service.human_readable_psbt(new_psbt_signed))
        return new_psbt_signed
    else:
        success, msg = wallet_service.sign_tx(tx, inscripts)
        if not success:
            log.error("Failed to sign transaction, quitting. Error msg: " + msg)
            return
        log.info("Got signed transaction:\n")
        log.info(human_readable_transaction(tx))
        actual_amount = amount if amount != 0 else total_inputs_val - fee_est
        sending_info = "Sends: " + amount_to_str(actual_amount) + \
            " to destination: " + destination
        if custom_change_addr:
            sending_info += ", custom change to: " + custom_change_addr
        log.info(sending_info)
        if not answeryes:
            if not accept_callback:
                if not cli_prompt_user_yesno('Would you like to push to the network?'):
                    log.info("You chose not to broadcast the transaction, quitting.")
                    return False
            else:
                accepted = accept_callback(human_readable_transaction(tx),
                                           destination, actual_amount, fee_est,
                                           custom_change_addr)
                if not accepted:
                    return False
        if change_label:
            try:
                wallet_service.set_address_label(change_addr, change_label)
            except UnknownAddressForLabel:
                # ignore, will happen with custom change not part of a wallet
                pass
        if jm_single().bc_interface.pushtx(tx.serialize()):
            txid = bintohex(tx.GetTxid()[::-1])
            successmsg = "Transaction sent: " + txid
            cb = log.info if not info_callback else info_callback
            cb(successmsg)
            txinfo = txid if not return_transaction else tx
            return txinfo
        else:
            errormsg = "Transaction broadcast failed!"
            cb = log.error if not error_callback else error_callback
            cb(errormsg)
            return False
